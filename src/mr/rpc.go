package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	MapTask = iota
	ReduceTask
	NoTaskAvailable
	AllTaskFinished
)

const (
	Ack = iota
	TaskExpired
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type DistributeArgs struct {
}

type DistributeReply struct {
	TaskType    int
	TaskNo      int
	ReduceNum   int
	TaskVersion int
	Files       []string
}

type CompleteArgs struct {
	TaskType    int
	TaskNo      int
	TaskVersion int
	Results     []string
}

type CompleteReply struct {
	Ack int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
