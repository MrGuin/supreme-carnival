package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type Kvs []KeyValue

func (a Kvs) Len() int {
	return len(a)
}
func (a Kvs) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}
func (a Kvs) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// call rpc to request for a task in a loop with time.Sleep
	for {
		//fmt.Println("new round, requesting for task...")
		reply := requestTask()
		switch reply.TaskType {
		case NoTaskAvailable:
			time.Sleep(100 * time.Millisecond)
		case AllTaskFinished:
			//fmt.Printf("all tasks done, worker exiting...")
			return
		case MapTask:
			taskId := reply.TaskNo
			reduceNum := reply.ReduceNum
			filename := reply.Files[0]
			//fmt.Printf("map task %d received\n", taskId)
			results := handleMapTask(taskId, reduceNum, filename, mapf)

			//notify master
			res, ack := notify(MapTask, taskId, reply.TaskVersion, results)
			if res == false || ack == TaskExpired {
				clean(results)
			}
		case ReduceTask:
			taskId := reply.TaskNo
			filenames := reply.Files
			//fmt.Printf("reduce task %d received\n", taskId)
			results := handleReduceTask(taskId, filenames, reducef)

			res, ack := notify(ReduceTask, taskId, reply.TaskVersion, results)
			//fmt.Printf("reduce task %d received\n", taskId)
			if res == false || ack == TaskExpired {
				clean(results)
			}
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func handleMapTask(taskId int, reduceNum int, filename string, mapf func(string, string) []KeyValue) (results []string) {
	//fmt.Printf("start handling map task %d...\n", taskId)
	// prepare
	var kvs []KeyValue
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// execute mapf
	kvs = mapf(filename, string(content))

	// write the intermediate KeyValues into the corresponding mr-X-Y files
	var hashedkvs = make([][]KeyValue, reduceNum)
	for _, kv := range kvs {
		bucket := ihash(kv.Key) % reduceNum
		hashedkvs[bucket] = append(hashedkvs[bucket], kv)
	}

	for r, kvs := range hashedkvs {
		pattern := fmt.Sprintf("mr-%d-%d#end", taskId, r)
		temp, err := ioutil.TempFile(".", pattern)
		if err != nil {
			log.Fatal(err)
		}
		results = append(results, temp.Name())
		storeKvs(kvs, temp)
		temp.Close()
	}
	return results
}

func handleReduceTask(taskId int, filenames []string, reducef func(string, []string) string) (results []string) {
	//fmt.Printf("start handling reduce task %d...\n", taskId)
	var kvs []KeyValue

	// extract file contents
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		file.Close()
	}

	// sort
	sort.Sort(Kvs(kvs))

	pattern := fmt.Sprintf("mr-out-%d#end", taskId)
	temp, err := ioutil.TempFile(".", pattern)
	if err != nil {
		log.Fatal(err)
	}

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[i].Key == kvs[j].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)
		fmt.Fprintf(temp, "%v %v\n", kvs[i].Key, output)
		i = j
	}

	results = append(results, temp.Name())
	temp.Close()
	return results
	// execute reducef

	// write the result into the mr-out-r file

}

func clean(temps []string) {
	for _, temp := range temps {
		os.Remove(temp)
	}
}

func storeKvs(kvs []KeyValue, file *os.File) {
	enc := json.NewEncoder(file)
	for _, kv := range kvs {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func extractKvs(kvs []KeyValue, filename string) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kvs = append(kvs, kv)
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// Request a task.
func requestTask() DistributeReply {
	args := DistributeArgs{}
	reply := DistributeReply{}
	call("Master.DistributeTask", &args, &reply)
	return reply
}

func notify(taskType int, taskNo int, taskVersion int, results []string) (bool, int) {
	//fmt.Printf("task type: %d no: %d finished, notifying master...\n", taskType, taskNo)
	args := CompleteArgs{
		TaskType:    taskType,
		TaskNo:      taskNo,
		TaskVersion: taskVersion,
		Results:     results,
	}
	reply := CompleteReply{}
	res := call("Master.CompleteTask", &args, &reply)
	return res, reply.Ack
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
