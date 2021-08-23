package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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
	var task Task
	// every worker generate a workId

	// get a map task, in this case just get filename
	for {
		askTask(struct{}{}, &task)
		fmt.Printf("task = %v \n", task)
		if task.TaskType == 1 {
			fmt.Println(" worker do map task : ", task.Id)
			task.OutputFileName = doMapTask(task, mapf)
		} else {
			fmt.Println(" worker do reduce task : ", task.Id)
			doReduceTask(task, reducef)
		}
		finishTask(&task, struct{}{})
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
		onameSuffix := ihash(intermediate[i].Key) % task.NReduce
		ofile, _ := os.OpenFile(oname + string(rune(onameSuffix)), os.O_CREATE | os.O_APPEND, 0666)
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


func askTask(args struct{}, replyTask *Task) {
	call("Master.AskTask", &args, replyTask)
}


func finishTask(requestTask *Task, reply struct{}) {
	call("Master.TaskFinish", requestTask, &reply)
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
