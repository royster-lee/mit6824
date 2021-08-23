package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Task struct {
	fileName  string
	id        int
	startTime time.Time
	status    TaskStatus
}

const (
	MapPhase = iota
	ReducePhase
)
type SchedulePhase int
type TaskStatus int

// Coordinator A laziest, worker-stateless, channel-based implementation of Coordinator
type Coordinator struct {
	files   []string
	nReduce int
	nMap    int
	phase   SchedulePhase
	tasks   []Task

	heartbeatCh chan heartbeatMsg
	reportCh    chan reportMsg
	doneCh      chan struct{}
}

type heartbeatMsg struct {
	response *HeartbeatResponse
	ok       chan struct{}
}

type reportMsg struct {
	request *ReportRequest
	ok      chan struct{}
}
type HeartbeatResponse struct {
	JobType int
}
type HeartbeatRequest struct {

}
type ReportRequest struct {

}
type ReportResponse struct {

}
// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Heartbeat(request *HeartbeatRequest, response *HeartbeatResponse) error {
	msg := heartbeatMsg{response, make(chan struct{})}
	c.heartbeatCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) Report(request *ReportRequest, response *ReportResponse) error {
	msg := reportMsg{request, make(chan struct{})}
	c.reportCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) schedule() {
	c.initMapPhase()
	for {
		select {
		case msg := <-c.heartbeatCh:
			switch c.phase {
			case MapPhase:


			}
			msg.ok <- struct{}{}
		case msg := <-c.reportCh:
			msg.ok <- struct{}{}
		}
	}
}

func (c *Coordinator) giveTask() Task{
	var task Task
	return task
}



//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	err := rpc.Register(c)
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
	go c.schedule()
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.server()
	return &c
}

