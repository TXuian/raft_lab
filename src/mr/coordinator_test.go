package mr

import (
	"reflect"
	"testing"
)

func TestCoordinator(t *testing.T) {

	assignTask := func(t *testing.T, got, want TaskRep) {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, but wanted %v", got, want)
		}
	}

	t.Run("test InitCoordinator", func(t *testing.T){
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
		got_1 := TaskRep{}
		got_2 := TaskRep{}
		got_3 := TaskRep{}
		got_4 := TaskRep{}
		got_5 := TaskRep{}
		got_6 := TaskRep{}
		got_7 := TaskRep{}
		got_8 := TaskRep{}
		got_9 := TaskRep{}

		c.HandleWorker(&request, &got_1)
		c.HandleWorker(&request, &got_2)
		c.HandleWorker(&request, &got_3)
		c.HandleWorker(&request, &got_4)
		c.HandleWorker(&request, &got_5)
		c.HandleWorker(&request, &got_6)
		c.HandleWorker(&request, &got_7)
		c.HandleWorker(&request, &got_8)
		err := c.HandleWorker(&request, &got_9)

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

		assignTask(t, got_1, want_map[got_1.Ifile_name_])
		assignTask(t, got_2, want_map[got_2.Ifile_name_])
		assignTask(t, got_3, want_map[got_3.Ifile_name_])
		assignTask(t, got_4, want_map[got_4.Ifile_name_])
		assignTask(t, got_5, want_map[got_5.Ifile_name_])
		assignTask(t, got_6, want_map[got_6.Ifile_name_])
		assignTask(t, got_7, want_map[got_7.Ifile_name_])
		assignTask(t, got_8, want_map[got_8.Ifile_name_])
	})
}