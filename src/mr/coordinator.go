package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type TaskStatus int
type TaskType int

const (
	Task_Idle = TaskStatus(0)
	Task_InProcess = TaskStatus(1)
	Task_Completed = TaskStatus(2)
)

const (
	MapTask = TaskType(0)
	ReduceTask = TaskType(1)
)

type TaskHandle struct {
	socket_name_ string
	status_ TaskStatus
	output_file_ []string
}

// TaskTable --> <filename, taskinfo>
type TaskTable map[string]TaskHandle

type Coordinator struct {
	// Your definitions here.
	map_task_table_ TaskTable
	reduce_task_table_ TaskTable
}

func (c *Coordinator) dispatch_map_task(files []string) {
	// num_task_ := len(files)
	
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) map_task_send(file, remote_name string) (ok bool) {
	// args := MapArgs{Input_file_name_: file}
	// reply := MapReply{}

	// do rpc
	return 
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) do_server(protocol string, sock string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove(sock)
	l, e := net.Listen(protocol, sock)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) server() {
	sock_name := coordinatorSock()
	c.do_server("unix", sock_name)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// files: input file that should deliver to map workers
// nReduce
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.


	c.server()
	return &c
}
