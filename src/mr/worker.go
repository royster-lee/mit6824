package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	var mu sync.Mutex
	mapWorker := func(){
		mu.Lock()
		filename := AskTask()
		mu.Unlock()
		if filename == "" {
			return
		}
		shuffleName := "shuffle-" + filename
		var intermediate []KeyValue
		fmt.Println("filename = ", filename)
		file, err := os.Open("../main/" + filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
		f, err := os.Create(shuffleName)
		if err != nil {
			log.Fatalf("Create shuffle error")
		}
		i, err := f.WriteString(fmt.Sprintf("%v", intermediate))
		fmt.Println("i = ", i)
		if err != nil {
			log.Fatalf("WriteString error")
		}
		f.Close()
		CompleteTask(shuffleName)
	}
	for i:=0; i<20; i++ {
		go mapWorker()
	}

	reducerWorker := func() {
		task := AskReduceTask()
		fmt.Println("reduce task = ", task)
	}
	for i := 0; i < 10; i++ {
		go reducerWorker()
	}
	time.Sleep(5 * time.Second)
}

func AskTask() string {
	args := struct{}{}
	reply := TaskReply{}
	call("Master.GiveTask", &args, &reply)
	return  reply.Filename
}

func AskReduceTask() string {
	args := struct{}{}
	reply := TaskReply{}
	call("Master.ReduceTask", &args, &reply)
	return  reply.Filename
}

func CompleteTask(shuffleName string) {
	args := TaskArgs{}
	args.Shuffle = shuffleName
	reply := struct{}{}
	call("Master.CompleteTask", &args, &reply)
}


//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	fmt.Println("sockname =", sockname)
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
