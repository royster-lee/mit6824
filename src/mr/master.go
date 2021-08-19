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
	mapTask []string
	mapTaskIndex int
	reduceChan chan string
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
func (m *Master) GiveTask(args *struct{}, reply *TaskReply) error {
	if m.mapTaskIndex == len(m.mapTask) {
		reply.Filename = ""
	} else {
		reply.Filename = m.mapTask[m.mapTaskIndex]
		m.mapTaskIndex++
	}

	return nil
}

func (m *Master) ReduceTask(args *struct{}, reply *TaskReply) error {
	one := <- m.reduceChan
	reply.Filename = one
	return nil
}

func (m *Master) CompleteTask(args *TaskArgs, reply *struct{}) error {
	m.reduceChan <- args.Shuffle
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
	m.mapTask = files
	m.reduceChan = make(chan string, nReduce)
	m.server()
	return &m
}

