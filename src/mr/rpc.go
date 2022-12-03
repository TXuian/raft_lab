package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskReq struct {}

type TaskRep struct { 
	Task_id_ int
	Task_type_ TaskType
	// map task
	Ifile_name_ string
	NReduce_ int
	// reduce task
	Ifile_name_list_ []string
}

type MapWorkDoneReq struct {
	Task_id_ int
	Intermediate_files_ []string
}

type ReduceWorkDoneReq struct {
	Task_id_ int
}

type WorkDoneRep struct {}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
