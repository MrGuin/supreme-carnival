package main

import (
	"../mr"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"plugin"
	"sort"
)

//
// simple sequential MapReduce.
//
// go run mysequential.go wc.so pg*.txt
//

// sorting kvs
type Kvs []mr.KeyValue

func (a Kvs) Len() int {
	return len(a)
}
func (a Kvs) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}
func (a Kvs) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mysequential xxx.go inputfiles...\n")
	}

	mapf, reducef := myLoadPlugin(os.Args[1])

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	var intermediate []mr.KeyValue
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kvs := mapf(filename, string(content))
		intermediate = append(intermediate, kvs...)
	}

	//
	// a big difference from real MapReduce is that all the intermediate date is in one place, intermediate[], rather than being partitioned into NxM buckets.
	//
	sort.Sort(Kvs(intermediate))

	oname := "mr-out-0"
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	//
	// call Reduce on each distinct key in intermediate[], and print the result to mr-out-0
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

//
// load the application Map and Reduce functions from a plugin file, e.g. ../mrapps/wc.so
//
func myLoadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	// open plugin file
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	// find and extract Map func
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.
	(func(string, string) []mr.KeyValue)
	// find and extract Reduce func
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
