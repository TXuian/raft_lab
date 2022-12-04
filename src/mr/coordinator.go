package mr

import (
	"fmt"
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
	input_file_ string
	status_ TaskStatus
	output_file_ []string
}

// TaskTable --> <filename, taskinfo>
type TaskTable map[int]TaskHandle

type Coordinator struct {
	// Your definitions here.
	map_task_table_ TaskTable
	reduce_task_table_ TaskTable
	nMap_ int
	nReduce_ int
	map_done_cnt_ int
	map_done_ bool
	reduce_done_cnt_ int
	all_done_ bool
}

type CoorErr string

func (ce CoorErr) Error() string {
	return string(ce)
}

const (
	err_no_task = CoorErr("[Coordinator] No task to dispatch")
)

// Your code here -- RPC handlers for the worker to call.
func GatherReduceFiles(file_list *[]string, map_table map[int]TaskHandle, reduce_idx int) {
	var map_idx int
	var cur_idx int
	for _, map_task := range map_table {
		for _, file_name := range map_task.output_file_ {
			fmt.Sscanf(file_name, "*/mr-%d-%d.txt", &map_idx, &cur_idx)
			if cur_idx == reduce_idx {
				*file_list = append(*file_list, file_name)
				break
			}
		}
	} 
}

func (c *Coordinator) HandleWorker(request *TaskReq, reply *TaskRep) error {
	// find a map task to dispatch
	for i, map_task := range c.map_task_table_ {
		if map_task.status_ == Task_Idle {
			reply.Task_id_ = i
			reply.Task_type_ = MapTask 
			reply.Ifile_name_ = map_task.input_file_
			reply.NReduce_ = c.nReduce_
			return nil
		}
	}
	// no map to dispatch
	if !c.map_done_ {
		return err_no_task
	}
	// dispatch reduce task
	for i, reduce_task := range c.reduce_task_table_ {
		if reduce_task.status_ == Task_Idle {
			reply.Task_id_ = i
			reply.Task_type_ = ReduceTask
			GatherReduceFiles(&(reply.Ifile_name_list_), c.map_task_table_, i)
		}
	}
	if !c.all_done_ {
		return err_no_task
	}
	return nil
}

func (c *Coordinator) MapWorkDone(reuqest *MapWorkDoneReq, reply *WorkDoneRep) error {
	return nil
}

func (c *Coordinator) ReduceWorkDone(request *ReduceWorkDoneReq, reply *WorkDoneRep) error {
	return nil
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
	return c.all_done_
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// files: input file that should deliver to map workers
// nReduce
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		map_done_cnt_: 0, 
		reduce_done_cnt_: 0, 
		nMap_: len(files), 
		nReduce_: nReduce,
		map_done_: false,
		all_done_: false,
	}

	// Your code here.
	// build map tasks
	for i, file_name := range files {
		c.map_task_table_[i] = TaskHandle{input_file_: file_name, status_: Task_Idle, output_file_: make([]string, 0)}
	}
	// build reduce tasks
	for i := 0; i < nReduce; i++ {
		c.reduce_task_table_[i] = TaskHandle{status_: Task_Idle}
	}

	c.server()
	return &c
}
