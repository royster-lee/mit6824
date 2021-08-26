package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
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
	reduceFiles			map[string]ByKey
	mappings			[]string // Map任务后生成的临时文件
	finishedMapNum		int32
	finishedReduceNum	int32
	done				bool
	mu 					*sync.Mutex
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
	Maping		string
}

const (
	MapJob = iota + 1
	ReduceJob
	WaitJob
	CompleteJob
)

// Your code here -- RPC handlers for the worker to call.

// 在任务队列找一个未处理的任务，如果任务超时了状态会重新变为未处理
func (m *Master) findTask(jobType int) *Task {
	var tasks []*Task
	if jobType == MapJob {
		tasks = m.mapTasks
	} else {
		tasks = m.reduceTasks
	}
	for _, task := range tasks {
		if task.State == TASK_INIT {
			task.State = TASK_PROCESSING
			go func(task *Task) {
				<- time.After(10 * time.Second)
				if task.State == TASK_PROCESSING {
					task.State = TASK_INIT
				}
			}(task)
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
		m.mu.Lock()
		task := m.findTask(MapJob)
		m.mu.Unlock()
		// 所有map任务状态变为2之后, m.sate 才会变为1,如果此处找不到未分配的map任务就说明有map任务在处理中
		if task == nil {
			responseMsg.JobType = WaitJob
		} else {
			responseMsg.Job.Index = task.Index
			responseMsg.Job.FileName = task.FileName
		}
	case MAP_FINISHED:
		responseMsg.JobType = ReduceJob
		m.mu.Lock()
		task := m.findTask(ReduceJob)
		m.mu.Unlock()
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
	fmt.Printf("report a job JobType =%d, index =%d ,ReduceFiles = %v\n", requestMsg.JobType, requestMsg.TaskIndex, requestMsg.Maping)
	switch requestMsg.JobType {
	case MapJob:
		if m.mapTasks[requestMsg.TaskIndex].State == TASK_PROCESSING {
			m.mapTasks[requestMsg.TaskIndex].State = TASK_DONE
			atomic.AddInt32(&m.finishedMapNum, 1)
			m.mappings = append(m.mappings, requestMsg.Maping)
		}
		if m.finishedMapNum == int32(m.nMap) {
			basicName := "shuffle-"
			// 遍历mapping 文件生成 shuffing 文件 传给reduce 任务
			for _, maping := range m.mappings {
				var intermediate []KeyValue
				// 生成shuffle
				file, _ := os.Open(maping)
				dec := json.NewDecoder(file)
				dec.Decode(&intermediate)
				i := 0
				for i < len(intermediate) {
					suffix := strconv.Itoa(ihash(intermediate[i].Key) % 10)
					oname := basicName + suffix
					m.reduceFiles[oname] = append(m.reduceFiles[oname], intermediate[i])
					i++
				}
				file.Close()
			}

			j := 0
			// 分配 reduce 任务
			for k, v := range m.reduceFiles {
				sfile, _ := os.Create(k)
				enc := json.NewEncoder(sfile)
				enc.Encode(&v)
				var task Task
				task.FileName = k
				task.Index = j
				m.reduceTasks = append(m.reduceTasks, &task)
				j++
				sfile.Close()
			}
			m.state = MAP_FINISHED
		}
	case ReduceJob:
		if m.reduceTasks[requestMsg.TaskIndex].State == TASK_PROCESSING {
			m.reduceTasks[requestMsg.TaskIndex].State = TASK_DONE
			atomic.AddInt32(&m.finishedReduceNum, 1)
			if m.finishedReduceNum == int32(m.nReduce) {
				m.state = REDUCE_FINISHED
				m.done = true
			}
		}
	default:
		if m.finishedMapNum == int32(m.nMap) {
			m.state = MAP_FINISHED
		}
		if m.finishedMapNum == int32(m.nReduce) {
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
	ret := m.done

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
	m.reduceFiles = make(map[string]ByKey)
	m.server()
	return &m
}

