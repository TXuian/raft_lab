package mr

import (
	"fmt"
	"reflect"
	"testing"
)

func TestCoordinator(t *testing.T) {

	assertTask := func(t *testing.T, got, want TaskRep) {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, but wanted %v", got, want)
		}
	}

	assertReduceTask := func(t *testing.T, got TaskRep) {
		if got.Task_type_ != ReduceTask {
			t.Errorf("Error Task Type")
		}

		var whatever int
		var reduce_id int
		for _, file := range got.Ifile_name_list_ {
			// TODO: set tmp file name pattern
			fmt.Sscanf(file, tmp_file_pattern, &whatever, &reduce_id)
			if reduce_id != got.Task_id_ {
				t.Errorf("Error tmp file for reduce task %d", got.Task_id_)
			}
		}
	}

	t.Run("test InitCoordinator init", func(t *testing.T){
		t.Helper()

		files := []string {
			"A", 
			"B", 
			"C", 
			"D", 
			"E", 
			"F", 
			"G", 
		}
		nReduce := 5
		got := InitCoordinator(&files, nReduce)

		want := Coordinator {
			nMap_: 7,
			nReduce_: 5,
			map_done_cnt_: 0,
			map_done_: false,
			reduce_done_cnt_: 0,
			all_done_: false,
		}
		want.map_task_table_ = make(TaskTable, 0)
		want.reduce_task_table_ = make(TaskTable, 0)

		want.map_task_table_[0] = &TaskHandle{input_file_: "A", status_: Task_Idle}
		want.map_task_table_[1] = &TaskHandle{input_file_: "B", status_: Task_Idle}
		want.map_task_table_[2] = &TaskHandle{input_file_: "C", status_: Task_Idle}
		want.map_task_table_[3] = &TaskHandle{input_file_: "D", status_: Task_Idle}
		want.map_task_table_[4] = &TaskHandle{input_file_: "E", status_: Task_Idle}
		want.map_task_table_[5] = &TaskHandle{input_file_: "F", status_: Task_Idle}
		want.map_task_table_[6] = &TaskHandle{input_file_: "G", status_: Task_Idle}

		want.reduce_task_table_[0] = &TaskHandle{status_: Task_Idle}
		want.reduce_task_table_[1] = &TaskHandle{status_: Task_Idle}
		want.reduce_task_table_[2] = &TaskHandle{status_: Task_Idle}
		want.reduce_task_table_[3] = &TaskHandle{status_: Task_Idle}
		want.reduce_task_table_[4] = &TaskHandle{status_: Task_Idle}

		if got.Equal(&want) {
			t.Errorf("\ngot        %v\nbut wanted %v", got, &want)
		}
	})

	t.Run("test HandleWorker: get mask task", func(t *testing.T){
		t.Helper()

		files := []string {
			"A", 
			"B", 
			"C", 
			"D", 
			"E", 
			"F", 
			"G", 
			"H",
		}
		nReduce := 5
		c := InitCoordinator(&files, nReduce)

		request := TaskReq{}
		mask_task_1 := TaskRep{}
		mask_task_2 := TaskRep{}
		mask_task_3 := TaskRep{}
		mask_task_4 := TaskRep{}
		mask_task_5 := TaskRep{}
		mask_task_6 := TaskRep{}
		mask_task_7 := TaskRep{}
		mask_task_8 := TaskRep{}
		mask_task_9 := TaskRep{}

		c.HandleWorker(&request, &mask_task_1)
		c.HandleWorker(&request, &mask_task_2)
		c.HandleWorker(&request, &mask_task_3)
		c.HandleWorker(&request, &mask_task_4)
		c.HandleWorker(&request, &mask_task_5)
		c.HandleWorker(&request, &mask_task_6)
		c.HandleWorker(&request, &mask_task_7)
		c.HandleWorker(&request, &mask_task_8)
		err := c.HandleWorker(&request, &mask_task_9)

		if err != err_no_task {
			t.Errorf("got %v, but wanted %v", err.Error(), err_no_task.Error())
		}

		want_map := make(map[string]TaskRep, 0)
		want_map["A"] = TaskRep{Task_id_: 0, Task_type_: MapTask, Ifile_name_: "A", NReduce_: 5}
		want_map["B"] = TaskRep{Task_id_: 1, Task_type_: MapTask, Ifile_name_: "B", NReduce_: 5}
		want_map["C"] = TaskRep{Task_id_: 2, Task_type_: MapTask, Ifile_name_: "C", NReduce_: 5}
		want_map["D"] = TaskRep{Task_id_: 3, Task_type_: MapTask, Ifile_name_: "D", NReduce_: 5}
		want_map["E"] = TaskRep{Task_id_: 4, Task_type_: MapTask, Ifile_name_: "E", NReduce_: 5}
		want_map["F"] = TaskRep{Task_id_: 5, Task_type_: MapTask, Ifile_name_: "F", NReduce_: 5}
		want_map["G"] = TaskRep{Task_id_: 6, Task_type_: MapTask, Ifile_name_: "G", NReduce_: 5}
		want_map["H"] = TaskRep{Task_id_: 7, Task_type_: MapTask, Ifile_name_: "H", NReduce_: 5}

		assertTask(t, mask_task_1, want_map[mask_task_1.Ifile_name_])
		assertTask(t, mask_task_2, want_map[mask_task_2.Ifile_name_])
		assertTask(t, mask_task_3, want_map[mask_task_3.Ifile_name_])
		assertTask(t, mask_task_4, want_map[mask_task_4.Ifile_name_])
		assertTask(t, mask_task_5, want_map[mask_task_5.Ifile_name_])
		assertTask(t, mask_task_6, want_map[mask_task_6.Ifile_name_])
		assertTask(t, mask_task_7, want_map[mask_task_7.Ifile_name_])
		assertTask(t, mask_task_8, want_map[mask_task_8.Ifile_name_])
	})
	

	t.Run("test HandleWorker: get reduce task", func(t *testing.T){
		t.Helper()

		files := []string { // size of files: 8
			"A", 
			"B", 
			"C", 
			"D", 
			"E", 
			"F", 
			"G", 
			"H",
		}
		nReduce := 5
		tmp_files := make(map[int][]string, 0) // <task_id, strings>  
		tmp_files[0] =  []string {"/mr-0-0.txt", "/mr-0-1.txt", "/mr-0-2.txt", "/mr-0-3.txt", "/mr-0-4.txt"}
		tmp_files[1] =  []string {"/mr-1-0.txt", "/mr-1-1.txt", "/mr-1-2.txt", "/mr-1-3.txt", "/mr-1-4.txt"}	
		tmp_files[2] =  []string {"/mr-2-0.txt", "/mr-2-1.txt", "/mr-2-2.txt", "/mr-2-3.txt", "/mr-2-4.txt"}
		tmp_files[3] =  []string {"/mr-3-0.txt", "/mr-3-1.txt", "/mr-3-2.txt", "/mr-3-3.txt", "/mr-3-4.txt"}
		tmp_files[4] =  []string {"/mr-4-0.txt", "/mr-4-1.txt", "/mr-4-2.txt", "/mr-4-3.txt", "/mr-4-4.txt"}
		tmp_files[5] =  []string {"/mr-5-0.txt", "/mr-5-1.txt", "/mr-5-2.txt", "/mr-5-3.txt", "/mr-5-4.txt"}
		tmp_files[6] =  []string {"/mr-6-0.txt", "/mr-6-1.txt", "/mr-6-2.txt", "/mr-6-3.txt", "/mr-6-4.txt"}
		tmp_files[7] =  []string {"/mr-7-0.txt", "/mr-7-1.txt", "/mr-7-2.txt", "/mr-7-3.txt", "/mr-7-4.txt"}
		reduce_input_files := make(map[int][]string, 0)
		reduce_input_files[0] = []string {"/mr-0-0.txt", "/mr-1-0.txt", "/mr-2-0.txt", "/mr-3-0.txt", "/mr-4-0.txt", "/mr-5-0.txt", "/mr-6-0.txt", "/mr-7-0.txt"}
		reduce_input_files[1] = []string {"/mr-0-1.txt", "/mr-1-1.txt", "/mr-2-1.txt", "/mr-3-1.txt", "/mr-4-1.txt", "/mr-5-1.txt", "/mr-6-1.txt", "/mr-7-1.txt"}
		reduce_input_files[2] = []string {"/mr-0-2.txt", "/mr-1-2.txt", "/mr-2-2.txt", "/mr-3-2.txt", "/mr-4-2.txt", "/mr-5-2.txt", "/mr-6-2.txt", "/mr-7-2.txt"}
		reduce_input_files[3] = []string {"/mr-0-3.txt", "/mr-1-3.txt", "/mr-2-3.txt", "/mr-3-3.txt", "/mr-4-3.txt", "/mr-5-3.txt", "/mr-6-3.txt", "/mr-7-3.txt"}
		reduce_input_files[4] = []string {"/mr-0-4.txt", "/mr-1-4.txt", "/mr-2-4.txt", "/mr-3-4.txt", "/mr-4-4.txt", "/mr-5-4.txt", "/mr-6-4.txt", "/mr-7-4.txt"}

		c := InitCoordinator(&files, nReduce)
		request := TaskReq{}
		map_tasks := make([]TaskRep, 8) 

		for i, _ := range map_tasks {
			c.HandleWorker(&request, &map_tasks[i])
			// fmt.Printf("task id: %d\n", map_request.Task_id_)
		}

		// reply map work done
		var work_done_reply WorkDoneRep;
		map_done_requests := []MapWorkDoneReq {
			{Task_id_: map_tasks[0].Task_id_, Intermediate_files_: tmp_files[map_tasks[0].Task_id_]},			
			{Task_id_: map_tasks[1].Task_id_, Intermediate_files_: tmp_files[map_tasks[1].Task_id_]},			
			{Task_id_: map_tasks[2].Task_id_, Intermediate_files_: tmp_files[map_tasks[2].Task_id_]},			
			{Task_id_: map_tasks[3].Task_id_, Intermediate_files_: tmp_files[map_tasks[3].Task_id_]},			
			{Task_id_: map_tasks[4].Task_id_, Intermediate_files_: tmp_files[map_tasks[4].Task_id_]},			
			{Task_id_: map_tasks[5].Task_id_, Intermediate_files_: tmp_files[map_tasks[5].Task_id_]},		
			{Task_id_: map_tasks[6].Task_id_, Intermediate_files_: tmp_files[map_tasks[6].Task_id_]},
			{Task_id_: map_tasks[7].Task_id_, Intermediate_files_: tmp_files[map_tasks[7].Task_id_]},
		}
		for _, done_request := range map_done_requests {
			c.MapWorkDone(&done_request, &work_done_reply)
		}
		if !c.map_done_ {
			t.Errorf("Error for map task done.")
		} 

		var err error
		reduce_task_requests := make([]TaskRep, nReduce)			
		extra_reduce_task := TaskRep{}
		for i, _ := range reduce_task_requests {
			err = c.HandleWorker(&request, &reduce_task_requests[i])
			if err != nil {
				t.Errorf("Error while dispatch reduce tasks.")
			}
		}
		err = c.HandleWorker(&request, &extra_reduce_task)
		if err != err_no_task {
			t.Errorf("got %s, but wanted %s", err.Error(), err_no_task.Error())
		}

		assertReduceTask(t, reduce_task_requests[0])
		assertReduceTask(t, reduce_task_requests[1])
		assertReduceTask(t, reduce_task_requests[2])
		assertReduceTask(t, reduce_task_requests[3])
		assertReduceTask(t, reduce_task_requests[4])

		reduce_done_request := make([]ReduceWorkDoneReq, nReduce)
		for i, reduce_done := range reduce_done_request {
			reduce_done.Task_id_ = i
			c.ReduceWorkDone(&reduce_done, &work_done_reply)
		}
		if !c.all_done_ {
			t.Error("Expect all_done, but got ", c.all_done_)
		}
	})

}