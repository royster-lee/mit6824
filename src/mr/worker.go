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

	// every worker generate a workId

	// get a map task, in this case just get filename
	for {
		var response ResponseMsg
		doHeartBreak(&response)
		switch response.JobType {
		case MapJob:
			doMapTask(response.Job, mapf)
		case ReduceJob:
			doReduceTask(response.Job, reducef)
		case WaitJob:
			fmt.Println("do wait job")
			time.Sleep(1 * time.Second)
		case CompleteJob:
			fmt.Println("worker return")
			return
		}
	}
}

func doReduceTask(task Task, reducef func(string, []string) string) {
	filename := task.FileName
	fmt.Println("worker do reduce task : ", filename)
	fp, _ := os.Open("../main/" + filename)
	dec := json.NewDecoder(fp)
	var intermediate []KeyValue
	dec.Decode(&intermediate)
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(task.Index)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	var requestMsg RequestMsg
	requestMsg.JobType = ReduceJob
	requestMsg.TaskIndex = task.Index
	doReport(&requestMsg)
}

func doMapTask(task Task, mapf func(string, string) []KeyValue) {
	filename := task.FileName
	println("worker do map task : ", filename)
	var intermediate []KeyValue
	file, _ := os.Open("../main/" + filename)
	content, _ := ioutil.ReadAll(file)
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	// 遍历kva, 生成shuffle文件保存到 reduceFiles, map任务结束时，应该遍历reduceFiles来生成reduceTasks
	mappingName := "mapping-" + strconv.Itoa(task.Index)
	var requestMsg RequestMsg
	ofile, _ := os.Create(mappingName)
	enc := json.NewEncoder(ofile)
	enc.Encode(&intermediate)
	requestMsg.Mapping = mappingName
	requestMsg.JobType = MapJob
	requestMsg.TaskIndex = task.Index
	doReport(&requestMsg)
}

func doHeartBreak(responseMsg *ResponseMsg) {
	call("Master.HeartBreak", &struct{}{}, responseMsg)
}
func doReport(requestMsg *RequestMsg) {
	call("Master.Report", requestMsg, &struct{}{})
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
