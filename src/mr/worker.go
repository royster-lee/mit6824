package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
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

const (
	MapJob = iota
	ReduceJob
	WaitJob
	CompleteJob
)
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

	for {
		response := doHeartbeat()
		log.Printf("Worker: receive coordinator's heartbeat %v \n", response)
		switch response.JobType {
		case MapJob:
			doMapTask(mapF, response)
		case ReduceJob:
			doReduceTask(reduceF, response)
		case WaitJob:
			time.Sleep(1 * time.Second)
		case CompleteJob:
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v", response.JobType))
		}
	}
}

func doReduceTask(task Task, reducef func(string, []string) string)  {
	fp, err := os.Open("../main/" + task.InputFileName)
	if err != nil {
		log.Printf("cannot open %v", task.InputFileName)
	}
	dec := json.NewDecoder(fp)
	var intermediate []KeyValue
	err = dec.Decode(&intermediate)
	if err != nil {
		log.Printf("cannot decode %v", task.InputFileName)
	}
	sort.Sort(ByKey(intermediate))
	i := 0
	oname := "mr-out-"

	for i < len(intermediate) {
		suffix := ihash(intermediate[i].Key) % task.NReduce
		ofile, _ := os.OpenFile(oname + strconv.Itoa(suffix), os.O_CREATE | os.O_APPEND | os.O_WRONLY, 0666)
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			log.Fatalf("error = %v \n", err)
		}

		i = j
	}

}

func doMapTask(task Task, mapf func(string, string) []KeyValue) string{
	shuffleName := "shuffle-" + task.InputFileName
	var intermediate []KeyValue
	file, err := os.Open("../main/" + task.InputFileName)
	if err != nil {
		log.Printf("cannot open %v", task.InputFileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot open %v", task.InputFileName)
	}
	err = file.Close()
	if err != nil {
		log.Printf("cannot close %v", task.InputFileName)
	}
	kva := mapf(task.InputFileName, string(content))
	intermediate = append(intermediate, kva...)
	shuffleFile, err := os.Create(shuffleName)
	if err != nil {
		log.Printf("cannot create %v", shuffleName)
	}
	enc := json.NewEncoder(shuffleFile)
	err = enc.Encode(&intermediate)
	if err != nil {
		log.Printf("cannot encode %v", shuffleName)
	}
	return shuffleName
}

func doHeartbeat() HeartbeatResponse{
	var response HeartbeatResponse
	var request HeartbeatRequest
	call("Coordinator.Heartbeat", &request, &response)
	return response
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
