package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkerInfo struct {
	id_ int
	task_type_ TaskType
}

func kva_to_str(kva *[]KeyValue) (buf string) {
	for _, kv := range *kva {
		buf += fmt.Sprintf("%v %v\n", kv.Key, kv.Value)
	}
	return 
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

func log_error(err error, format string, v ...interface{}) {
	if err != nil {
		log.Fatalf(format, v...)
	}
}

/************************* Map Func ****************************/
//
// main/mrworker.go calls this function.
// tmp file name: mr-X-Y
func MapInput(mapf func(string, string) []KeyValue, ifile io.Reader, ifile_name string) []KeyValue {
	content, err := ioutil.ReadAll(ifile)
	log_error(err, "[MAP] Read input file %v failed", ifile_name)
	return mapf(ifile_name, string(content))
}

func MapOutput(ofiles io.Writer, intermediate_kva *[]KeyValue) {
	// 	append kv to according file
	// sort.Sort(ByKey(*intermediate_kva))
	fmt.Fprint(ofiles,  kva_to_str(intermediate_kva))
}

func DoMap(mapf func(string, string) []KeyValue, ifile_name string, ofile_name_list []string, nReduce int) bool {
	// assert length of ofile name list
	if len(ofile_name_list) != nReduce {
		return false
	}
	// 1. handle input file
	ifile, err := os.Open((ifile_name))
	defer ifile.Close()
	log_error(err, "cannot open %v", ifile_name)
	kva := MapInput(mapf, ifile, ifile_name)

	// 2. handle output file
	// 	generate intermediate kva [0, ..., nReduce)
	intermediate_kva := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		reduce_id := ihash(kv.Key) % nReduce
		intermediate_kva[reduce_id] = append(intermediate_kva[reduce_id], kv)
	}

	// 	open nReduce files
	for i := 0; i < nReduce; i++ {
		// open file
		ofile, err := os.OpenFile(ofile_name_list[i], os.O_CREATE|os.O_APPEND|os.O_RDWR, 0744)
		defer ofile.Close()
		log_error(err, "cannot open %v", ofile_name_list[i])
		MapOutput(ofile, &intermediate_kva[i])
	}

	// uncomment to send the Example RPC to the coordinator.
	return true
}

/************************* Reduce Func ****************************/
// tmp file name: mr-Y-O
func ReduceInput(ifile io.Reader, intermediate_kva *[]KeyValue) {
	// num_file := len(ifiles)
	var key, value string
	for {
		n, err := fmt.Fscanf(ifile, "%s %s\n", &key, &value)
		if err == io.EOF || n == 0 { break }
		*intermediate_kva = append(*intermediate_kva, KeyValue{Key: key, Value: value})
	}
}

func ReduceOutput(reducef func(string, []string) string, ofile io.Writer, intermediate_kva *[]KeyValue) bool {
	i := 0
	for i < len(*intermediate_kva) {
		j := i + 1
		for j < len(*intermediate_kva) && (*intermediate_kva)[j].Key == (*intermediate_kva)[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, (*intermediate_kva)[k].Value)
		}
		output := reducef((*intermediate_kva)[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", (*intermediate_kva)[i].Key, output)

		i = j
	}
	return true
}

func DoReduce(reducef func(string, []string) string, ifile_name_list []string, ofile_name string, id int) bool {
	// num_file := len(ifile_name_list)
	intermediate_kva := make([]KeyValue, 0)
	for _, fname := range(ifile_name_list) {
		ifile, err := os.Open(fname)
		defer ifile.Close()
		log_error(err, "[REDUCE] Failed to open file %v", fname)
		ReduceInput(ifile, &intermediate_kva)
	}
	sort.Sort(ByKey(intermediate_kva))
	
	ofile_tmp_name := fmt.Sprintf("src/main/mr-tmp/mr-r%d.txt", id)
	ofile_tmp, err := os.OpenFile(ofile_tmp_name, os.O_CREATE | os.O_APPEND | os.O_RDWR, 0744)
	log_error(err, "[REDUCE] Failed to create tmp file")
	ok := ReduceOutput(reducef, ofile_tmp, &intermediate_kva)

	os.Rename(ofile_tmp_name, ofile_name)
	return ok
}

// singleton: worker info
var instance *WorkerInfo
var once sync.Once
 
func GetWorkerInfo() *WorkerInfo {
    once.Do(func() {
        instance = &WorkerInfo{}
    })
    return instance
}

func RequestForWork(mapf func(string, string) []KeyValue, reducef func(string, []string) string) error {
	// do remote call
	req := TaskReq{}
	rep := TaskRep{}
	ok := call("Coordinator.HandleWorker", req, &rep)
	if !ok {
		log.Fatalf("[Worker] Request for task failed!\n")
	}

	// handle work
	if rep.Task_type_ == MapTask { // do Map task
		ofile_name_list := make([]string, rep.NReduce_)
		for i, _ := range ofile_name_list {
			ofile_name_list[i] = fmt.Sprintf("src/main/mr-tmp/mr-%d-%d.txt", rep.Task_id_, i)
		}
		DoMap(mapf, rep.Ifile_name_, ofile_name_list, rep.NReduce_)

		task_done_req := MapWorkDoneReq{Task_id_: rep.Task_id_, Intermediate_files_: ofile_name_list}
		task_done_rep := WorkDoneRep{}
		ok = call("Coordinator.MapWorkDone", &task_done_req, &task_done_rep)

	} else if rep.Task_type_ == ReduceTask { // do reduce task
		ofile_name_list := fmt.Sprintf("src/main/mr-tmp/mr-out-%d", rep.Task_id_)
		DoReduce(reducef, rep.Ifile_name_list_, ofile_name_list, rep.Task_id_)

		task_done_req := ReduceWorkDoneReq{Task_id_: rep.Task_id_}
		task_done_rep := WorkDoneRep{}
		ok = call("Coordinator.ReduceWorkDone", &task_done_req, &task_done_rep)
	}

	if !ok {
		log.Fatalf("[Worker] Reply of task failed!\n")
	}
	
	return nil
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		RequestForWork(mapf, reducef)
	}
}



//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
