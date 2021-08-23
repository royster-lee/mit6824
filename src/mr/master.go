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
	mapTaskChan 	chan Task
	reduceTaskChan 	chan Task
	fileCount		int
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
func (m *Master) GiveMapTask(_ struct{}, ctx *WorkerCtx) error {
	mapTask := <- m.mapTaskChan
	fmt.Println("give " + ctx.WorkId + " : " + mapTask.FileName)
	fmt.Println("xxxxxxxxx ", cap(ctx.MapTaskChan))
	ctx.MapTaskChan <- mapTask
	return nil
}

func (m *Master) CompleteTask(_ struct{}, ctx *WorkerCtx) error {
	fmt.Println(ctx.WorkId + " created a shuffle : " + ctx.ShuffleName)
	m.fileCount--
	if m.fileCount == 0 {
		ctx.Done <- 1
		m.Done()
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
	m.mapTaskChan = make(chan Task, length)
	var task Task
	for i:=0; i<length; i++ {
		task.FileName = files[i]
		m.mapTaskChan <- task
	}
	m.reduceTaskChan = make(chan Task, nReduce)
	m.fileCount = len(files)
	m.server()
	return &m
}

