package mr

import (
	"fmt"
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	nReduce             int
	nMap                int
	mapTasks            []*Task
	reduceTasks         []*Task
	state               int // MASTER_INIT;MAP_FINISHED;REDUCE_FINISHED
	reduceFiles			map[string]bool
	finishedMapNum		int
	finishedReduceNum	int
}

const (
	MASTER_INIT = iota
	MAP_FINISHED
	REDUCE_FINISHED
)

const (
	TASK_INIT = iota
	TASK_PROCESSING
	TASK_DONE
)

type Task struct {
	State     	int // TASK_INIT;TASK_PROCESSING;TASK_DONE
	FileName  	string
	Index      	int
	StartTime   int64
}

type ResponseMsg struct {
	NReduce 	int
	JobType		int
	Job			Task
}
type HeartbeatResponse struct {
	JobType int
}
type RequestMsg struct {
	JobType		int
	TaskIndex	int
	ReduceFiles	[]string
}

const (
	MapJob = iota + 1
	ReduceJob
	WaitJob
	CompleteJob
)

// Your code here -- RPC handlers for the worker to call.

// 在任务队列找一个未处理的任务，如果任务超时了状态会重新变为未处理
func (m *Master) findMapTask() *Task {
	for _, task := range m.mapTasks {
		if task.State == TASK_INIT {
			task.State = TASK_PROCESSING
			return task
		}
	}
	return nil
}

func (m *Master) findReduceTask() *Task {
	for _, task := range m.reduceTasks {
		if task.State == TASK_INIT {
			task.State = TASK_PROCESSING
			return task
		}
	}
	return nil
}


func (m *Master) HeartBreak(_ *struct{}, responseMsg *ResponseMsg) error {
	responseMsg.NReduce = m.nReduce
	switch m.state {
	case MASTER_INIT:
		responseMsg.JobType = MapJob
		task := m.findMapTask()
		// 所有map任务状态变为2之后, m.sate 才会变为1,如果此处找不到未分配的map任务就说明有map任务在处理中
		if task == nil {
			responseMsg.JobType = WaitJob
		} else {
			responseMsg.Job.Index = task.Index
			responseMsg.Job.FileName = task.FileName
		}
	case MAP_FINISHED:
		responseMsg.JobType = ReduceJob
		task := m.findReduceTask()
		if task == nil {
			responseMsg.JobType = WaitJob
		} else {
			responseMsg.Job.Index = task.Index
			responseMsg.Job.FileName = task.FileName
		}
	case REDUCE_FINISHED:
		responseMsg.JobType = CompleteJob
	}
	return nil
}


func (m *Master) Report(requestMsg *RequestMsg, _ *struct{}) error {
	// we assumption master will not crash, complete task
	fmt.Printf("report a job JobType =%d, index =%d ,ReduceFiles = %v state = %d\n", requestMsg.JobType, requestMsg.TaskIndex, requestMsg.ReduceFiles,m.mapTasks[requestMsg.TaskIndex].State)
	switch requestMsg.JobType {
	case MapJob:
		if m.mapTasks[requestMsg.TaskIndex].State == TASK_PROCESSING {
			m.mapTasks[requestMsg.TaskIndex].State = TASK_DONE
			m.finishedMapNum++
			for _, reduceFile := range requestMsg.ReduceFiles {
				m.reduceFiles[reduceFile] = true
			}
		}
		if m.finishedMapNum == m.nMap {
			m.state = MAP_FINISHED
			i := 0
			// 分配 reduce 任务
			for k, _ := range m.reduceFiles {
				var task Task
				task.FileName = k
				task.Index = i
				m.reduceTasks = append(m.reduceTasks, &task)
				i++
			}
		}
	case ReduceJob:
		if m.reduceTasks[requestMsg.TaskIndex].State == TASK_PROCESSING {
			m.reduceTasks[requestMsg.TaskIndex].State = TASK_DONE
			m.finishedReduceNum++
			if m.finishedMapNum == m.nReduce {
				m.state = MAP_FINISHED
			}
			m.state = REDUCE_FINISHED
		}
	default:
		if m.finishedMapNum == m.nMap {
			m.state = MAP_FINISHED
		}
		if m.finishedMapNum == m.nReduce {
			m.state = MAP_FINISHED
		}

	}
	return nil
}




//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	err := rpc.Register(m)
	if err != nil {
		return 
	}
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	fmt.Println("sockname =", sockname)
	err = os.Remove(sockname)
	if err != nil {
		return 
	}
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		err := http.Serve(l, nil)
		if err != nil {
		}
	}()
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	length := len(files)

	for i:=0; i< length; i++ {
		var task Task
		task.FileName = files[i]
		task.Index = i
		m.mapTasks = append(m.mapTasks, &task)
	}
	m.nMap = 8
	m.nReduce = nReduce
	m.reduceFiles = make(map[string]bool)
	m.server()
	return &m
}

