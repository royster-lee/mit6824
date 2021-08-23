package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
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
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// initial worker struct
	var wCtx WorkerCtx
	// every worker generate a workId
	r := rand.New(rand.NewSource(time.Now().Unix()))
	bytes := make([]byte, 10)
	for i := 0; i < 10; i++ {
		b := r.Intn(26) + 65
		bytes[i] = byte(b)
	}
	wCtx.WorkId = string(bytes)
	fmt.Println("wCtx.WorkId : ", wCtx.WorkId)
	// get a map task, in this case just get filename
	wCtx.Done = make(chan int, 1)
	wCtx.mapTaskChan = make(chan Task, 1)
	for {
		select {
		case maptask := <-wCtx.mapTaskChan:
			fmt.Println("do map task")
			doMapTask(&wCtx, maptask.FileName, mapf)
		case <-wCtx.Done:
			fmt.Println("worker return")
			return
		default:
			fmt.Println("askMapTask")
			askMapTask(&wCtx)
		}
	}
}

func doMapTask(wCtx *WorkerCtx, filename string, mapf func(string, string) []KeyValue){
	shuffleName := "shuffle-" + filename
	var intermediate []KeyValue
	file, err := os.Open("../main/" + filename)
	if err != nil {
		log.Printf("cannot open %v", filename)
		wCtx.ErrCh <- err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot open %v", filename)
		wCtx.ErrCh <- err
	}
	err = file.Close()
	if err != nil {
		log.Printf("cannot close %v", filename)
		wCtx.ErrCh <- err
	}
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	shuffleFile, err := os.Create(shuffleName)
	if err != nil {
		log.Printf("cannot create %v", shuffleName)
		wCtx.ErrCh <- err
	}
	enc := json.NewEncoder(shuffleFile)
	err = enc.Encode(&intermediate)
	if err != nil {
		log.Printf("cannot encode %v", shuffleName)
		wCtx.ErrCh <- err
	}
	wCtx.ShuffleName = shuffleName
	CompleteTask(wCtx)
}


func askMapTask(wCtx *WorkerCtx) {
	call("Master.GiveMapTask", wCtx, wCtx)
}


func CompleteTask(wCtx *WorkerCtx) {
	call("Master.CompleteTask", wCtx, wCtx)
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
