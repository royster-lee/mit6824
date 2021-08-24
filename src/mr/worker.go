package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

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

	// Your worker implementation here.
	// uncomment to send the Example RPC to the master.
	// CallExample()

	// initial worker struct
	var response ResponseMsg
	// every worker generate a workId

	// get a map task, in this case just get filename
	for {
		doHeartBreak(&response)

		switch response.JobType {
		case MapJob:
			doMapTask(response.Job, mapf)
		case ReduceJob:
			doReduceTask(response.Job, reducef)
		case WaitJob:
			time.Sleep(1 * time.Second)
		case CompleteJob:
			fmt.Println("worker return")
		}
	}
}

func doReduceTask(task Task, reducef func(string, []string) string) {
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
		ofile, _ := os.OpenFile(oname+strconv.Itoa(suffix), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
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

func doMapTask(task Task, mapf func(string, string) []KeyValue) string {
	shuffleName := "shuffle-" + task.InputFileName
	var intermediate []KeyValue
	file, _ := os.Open("../main/" + task.InputFileName)
	content, _ := ioutil.ReadAll(file)
	file.Close()
	kva := mapf(task.InputFileName, string(content))
	intermediate = append(intermediate, kva...)
	// 遍历kva, 生成shuffle文件保存到 reduceFiles, map任务结束时，应该遍历reduceFiles来生成reduceTasks

	shuffleFile, _ := os.Create(shuffleName)
	enc := json.NewEncoder(shuffleFile)
	enc.Encode(&intermediate)
	return shuffleName
}

func doHeartBreak(responseMsg *ResponseMsg) {
	call("Master.HeartBreak", &struct{}{}, responseMsg)
}
func doReport(requestMsg *RequestMsg) {
	call("Master.HeartBreak", requestMsg, &struct{}{})
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
