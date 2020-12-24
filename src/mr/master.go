package mr

import (
	"log"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	mapTasks     []*mapTask
	reduceTasks  []*reduceTask
	mu           sync.Mutex
	reduceNum    int
	intermediate [][]string
	currentStage int
	stageCond    *sync.Cond
}

type mapTask struct {
	index    int
	filename string
	state    int
	done     chan struct{}
	version  int
}

type reduceTask struct {
	index     int
	filenames []string
	state     int
	done      chan struct{}
	version   int
}

// task state
const (
	Idle = iota
	Assigned
	Complete
)

// currentStage values
const (
	MapStage = iota
	ReduceStage
	AllComplete
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// RPC handlers.
//

// request for a task
func (m *Master) DistributeTask(args *DistributeArgs, reply *DistributeReply) error {
	//fmt.Println("distributing task...")
	m.mu.Lock()
	defer m.mu.Unlock()
	switch m.currentStage {
	case MapStage:
		if mt := m.getAMapTask(); mt != nil {
			// generate reply
			reply.TaskType = MapTask
			reply.TaskNo = mt.index
			reply.ReduceNum = m.reduceNum
			reply.TaskVersion = mt.version
			reply.Files = []string{mt.filename}
			go m.monitorTimeout(mt)
		} else { // no map task available, return
			reply.TaskType = NoTaskAvailable
		}
	case ReduceStage:
		if rt := m.getAReduceTask(); rt != nil {
			reply.TaskType = ReduceTask
			reply.TaskNo = rt.index
			reply.ReduceNum = m.reduceNum
			reply.TaskVersion = rt.version
			reply.Files = rt.filenames
			go m.monitorTimeout(rt)
		} else {
			reply.TaskType = NoTaskAvailable
		}
	case AllComplete:
		reply.TaskType = AllTaskFinished
	}
	return nil
}

// complete the task
func (m *Master) CompleteTask(args *CompleteArgs, reply *CompleteReply) error {
	//fmt.Println("one task finished, analyzing...")
	switch args.TaskType {
	case MapTask:
		m.mu.Lock()
		index := args.TaskNo
		mt := m.mapTasks[index]
		taskVersion := args.TaskVersion
		if taskVersion == mt.version {
			mt.state = Complete
			reply.Ack = Ack
			//fmt.Printf("map task %d finished, state updated, notifying task's monitor...", mt.index)

			mt.done <- struct{}{}

			tempFiles := args.Results
			var inters []string
			// rename temporary files and save them to m.intermediate
			for _, temp := range tempFiles {
				i := strings.Index(temp, "#end")
				newName := temp[:i]
				if err := os.Rename(temp, newName); err != nil {
					log.Fatalf("cannot rename %v: %v", temp, err)
				}
				inters = append(inters, newName)
			}
			m.intermediate[index] = inters
			m.mu.Unlock()
			m.stageCond.Broadcast() // awake stage monitor
		} else { // task expired
			m.mu.Unlock()
			reply.Ack = TaskExpired
		}
	case ReduceTask:
		m.mu.Lock()
		index := args.TaskNo
		rt := m.reduceTasks[index]
		taskVersion := args.TaskVersion
		if taskVersion == rt.version {
			//rt.done <- struct{}{}

			rt.state = Complete
			reply.Ack = Ack
			tempName := args.Results[0]

			i := strings.Index(tempName, "#end")
			newName := tempName[:i]
			if err := os.Rename(tempName, newName); err != nil {
				log.Fatalf("cannot rename %v: %v", tempName, err)
			}
			m.mu.Unlock()
			m.stageCond.Broadcast()
		} else {
			m.mu.Unlock()
			reply.Ack = TaskExpired
		}
	}
	return nil
}

//
// Private methods.
//

// monitor timeout
func (m *Master) monitorTimeout(task interface{}) {
	switch t := task.(type) {
	case *mapTask:
		select {
		case <-time.After(10 * time.Second): // timeout
			m.mu.Lock()
			t.version++
			t.state = Idle
			m.mu.Unlock()
			//fmt.Printf("map task %d timeout, recycled", t.index)
		case <-t.done:
			//fmt.Printf("map task %d finished, monitor quit", t.index)
			return
		}
	case *reduceTask:
		select {
		case <-time.After(10 * time.Second):
			m.mu.Lock()
			t.version++
			t.state = Idle
			m.mu.Unlock()
			//fmt.Printf("reduce task %d timeout, recycled", t.index)
		case <-t.done:
			//fmt.Printf("map task %d finished, monitor quit", t.index)
			return
		}
	}
}

// monitor stage
func (m *Master) stageMonitor() {
	m.mu.Lock()
	for !m.checkMapAllComplete() {
		m.stageCond.Wait()
	}
	m.currentStage = ReduceStage
	// prepare reduce tasks
	m.prepareReduceTasks()

	for !m.checkReduceAllComplete() {
		m.stageCond.Wait()
	}
	m.currentStage = AllComplete
	m.mu.Unlock()
	//fmt.Println("all tasks done, good job!")
}

// assuming lock held
func (m *Master) getAMapTask() *mapTask {
	for _, mt := range m.mapTasks {
		if mt.state == Idle {
			mt.state = Assigned
			//fmt.Printf("map task %d assigned", mt.index)
			return mt
		}
	}
	return nil
}

// assuming lock held
func (m *Master) getAReduceTask() *reduceTask {
	for _, rt := range m.reduceTasks {
		if rt.state == Idle {
			rt.state = Assigned
			return rt
		}
	}
	return nil
}

// check whether map stage is complete
// assuming lock held
func (m *Master) checkMapAllComplete() bool {
	for _, mt := range m.mapTasks {
		if mt.state != Complete {
			return false
		}
	}
	return true
}

// check whether reduce stage is complete
// assuming lock held
func (m *Master) checkReduceAllComplete() bool {
	for _, rt := range m.reduceTasks {
		if rt.state != Complete {
			return false
		}
	}
	return true
}

func (m *Master) prepareMapTasks(files []string) {
	i := 0
	for _, filename := range files {
		m.mapTasks[i] = &mapTask{
			index:    i,
			filename: filename,
			state:    Idle,
			done:     make(chan struct{}),
			version:  0,
		}
		i++
	}
}

func (m *Master) prepareReduceTasks() {
	for r := 0; r < m.reduceNum; r++ {
		var files []string
		for i := 0; i < len(m.intermediate); i++ {
			files = append(files, m.intermediate[i][r])
		}
		m.reduceTasks[r] = &reduceTask{
			index:     r,
			filenames: files,
			state:     0,
			done:      make(chan struct{}),
			version:   0,
		}
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	ret = m.currentStage == AllComplete
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		mapTasks:     make([]*mapTask, len(files)),
		reduceTasks:  make([]*reduceTask, nReduce),
		mu:           sync.Mutex{},
		reduceNum:    nReduce,
		intermediate: make([][]string, len(files)),
		currentStage: 0,
		stageCond:    nil,
	}
	m.stageCond = sync.NewCond(&m.mu)

	// Your code here.
	m.prepareMapTasks(files)

	m.server()
	go m.stageMonitor()
	return &m
}
