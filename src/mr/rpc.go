package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskReply struct {
	Filename 	string
	Done 		int
}

type TaskArgs struct {
	WorkId 		string
	ShuffleName	string
}

type WorkerCtx struct {
	WorkId 		string
	ErrCh 		chan error
	MapTaskChan	chan Task
	Done    	chan int
}

type Task struct {
	FileName string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
