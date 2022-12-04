package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"reflect"
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
	count_down_ int
}

// TaskTable --> <filename, taskinfo>
type TaskTable map[int]*TaskHandle

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

func (lh *Coordinator) Equal(rh *Coordinator) bool {
	for i, lh_map_handle := range lh.map_task_table_ {
		if !reflect.DeepEqual(lh_map_handle, rh.map_task_table_[i]) {
			return false
		}
	}

	for i, lh_reduce_handle := range lh.reduce_task_table_ {
		if !reflect.DeepEqual(lh_reduce_handle, rh.reduce_task_table_[i]) {
			return false
		}
	}

	return (
		lh.nMap_ == rh.nMap_ &&
		lh.nReduce_ == rh.nReduce_ &&
		lh.map_done_ == rh.map_done_ &&
		lh.map_done_cnt_ == rh.map_done_cnt_ &&
		lh.reduce_done_cnt_ == rh.reduce_done_cnt_ &&
		lh.all_done_ == rh.all_done_)
}

type CoorErr string

func (ce CoorErr) Error() string {
	return string(ce)
}

const (
	err_no_task = CoorErr("[Coordinator] No task to dispatch")
)

// Your code here -- RPC handlers for the worker to call.
func GatherReduceFiles(file_list *[]string, map_table TaskTable, reduce_idx int) {
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
			// handle task info
			map_task.status_ = Task_InProcess
			map_task.count_down_ = 10
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
			reduce_task.status_ = Task_InProcess
			reduce_task.count_down_ = 10
			return nil
		}
	}
	if !c.all_done_ {
		return err_no_task
	}
	return nil
}

func (c *Coordinator) MapWorkDone(request *MapWorkDoneReq, reply *WorkDoneRep) error {
	c.map_task_table_[request.Task_id_].status_ = Task_Completed
	c.map_task_table_[request.Task_id_].output_file_ = request.Intermediate_files_
	c.map_done_cnt_++
	if c.map_done_cnt_ == c.nMap_ {
		c.map_done_ = true
	}
	return nil
}

func (c *Coordinator) ReduceWorkDone(request *ReduceWorkDoneReq, reply *WorkDoneRep) error {
	c.reduce_task_table_[request.Task_id_].status_ = Task_Completed
	c.reduce_done_cnt_++
	if c.reduce_done_cnt_ == c.nReduce_ {
		c.all_done_ = true
	}
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
func InitCoordinator(files *[]string, nReduce int) *Coordinator {
	c := Coordinator{
		map_done_cnt_: 0, 
		reduce_done_cnt_: 0, 
		nMap_: len(*files), 
		nReduce_: nReduce,
		map_done_: false,
		all_done_: false,
	}

	// Your code here.
	c.map_task_table_ = make(TaskTable, 0)
	c.reduce_task_table_ = make(TaskTable, 0)
	// build map tasks
	for i, file_name := range *files {
		c.map_task_table_[i] = &TaskHandle{input_file_: file_name, status_: Task_Idle, output_file_: make([]string, 0)}
	}
	// build reduce tasks
	for i := 0; i < nReduce; i++ {
		c.reduce_task_table_[i] = &TaskHandle{status_: Task_Idle}
	}

	return &c
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := InitCoordinator(&files, nReduce)

	c.server()
	return c
}
