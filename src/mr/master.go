package mr

import (
	"fmt"
	"log"
	"sync/atomic"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	taskCh 		chan Task
	fileCount	int32
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
func (m *Master) GiveMapTask(args *TaskArgs, reply *TaskReply) error {
	task := <- m.taskCh
	atomic.AddInt32(&m.fileCount, -1)
	fmt.Println("filecount : ", m.fileCount)
	if m.fileCount != -1 {
		fmt.Println("give " + args.WorkId + " : " + task.FileName)
		reply.Filename = task.FileName
		reply.Done = 0
	} else {
		reply.Done = 1
		m.Done()
	}

	return nil
}

func (m *Master) CompleteTask(args *TaskArgs, reply *TaskReply) error {
	fmt.Println(args.WorkId + " generate a shuffle : " + args.ShuffleName)
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
	m.taskCh = make(chan Task, length)
	var task Task
	for i:=0; i< length; i++ {
		fileName := files[i]
		task.FileName = fileName
		m.taskCh <- task
	}
	m.fileCount = int32(length)
	m.server()
	return &m
}

