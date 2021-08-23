package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mu sync.Mutex

type Master struct {
	// Your definitions here.
	nReduce             int
	nMap                int
	mapTasks            []Task
	reduceTasks         []Task
	state               int // MASTER_INIT;MAP_FINISHED;REDUCE_FINISHED
	mapTaskFinishNum    int
	reduceTaskFinishNum int
}

type Task struct {
	State          int // TASK_INIT;TASK_PROCESSING;TASK_DONE
	InputFileName  string
	Id             int
	OutputFileName string
	TaskType       int // MAP_TASK;REDUCE_TASK
	NReduce        int
	NMap           int
	StartTime      int64
}


// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) AskTask(_ *struct{}, replyTask *Task) error {
	if m.state == 0 {
		if len(m.mapTasks) == 0 {
			return nil
		}
		mapTask := m.mapTasks[0]
		m.mapTasks = m.mapTasks[1:]
		replyTask.InputFileName = mapTask.InputFileName
		replyTask.Id = mapTask.Id
		replyTask.NReduce = mapTask.NReduce
		replyTask.TaskType = mapTask.TaskType
		replyTask.NMap = mapTask.NMap
	} else {
		if len(m.reduceTasks) == 0 {
			return nil
		}
		reduceTask := m.reduceTasks[0]
		m.reduceTasks = m.reduceTasks[1:]
		replyTask.InputFileName = reduceTask.InputFileName
		replyTask.Id = reduceTask.Id
		replyTask.NReduce = reduceTask.NReduce
		replyTask.TaskType = reduceTask.TaskType
		replyTask.NMap = reduceTask.NMap
	}
	return nil
}



func (m *Master) TaskFinish(requestTask *Task, _ *struct{}) error {
	// we assumption master will not crash, complete task
	if requestTask.TaskType == 1 {
		m.mapTaskFinishNum++
		var task Task
		task.TaskType = 2
		task.InputFileName = requestTask.OutputFileName
		task.Id = requestTask.Id
		task.NReduce = requestTask.NReduce
		m.reduceTasks = append(m.reduceTasks, task)
		if m.mapTaskFinishNum == m.nMap {
			fmt.Println("map phrase finish")
			m.state = 1
		}
	} else {
		m.reduceTaskFinishNum++
		if m.reduceTaskFinishNum == m.nReduce {
			fmt.Println("reduce phrase finish")
			m.state = 2
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
	ret := m.state == 2

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
	var task Task
	for i:=0; i< length; i++ {
		task.InputFileName = files[i]
		task.TaskType = 1
		task.Id = i
		task.NMap = 8
		task.NReduce = nReduce
		m.mapTasks = append(m.mapTasks, task)
	}
	m.nReduce = 8
	m.nMap = 8
	m.server()
	return &m
}

