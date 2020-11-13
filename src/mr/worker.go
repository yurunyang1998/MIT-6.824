package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"encoding/json"
)

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	applyTask(mapf, reducef)
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

func applyTask(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	args := ExampleArgs{}
	args.X = 99
	reply := Task{}
	call("Master.AssignTask", &args, &reply)
	if reply.TaskType == 0 { //map task

		mapWork(reply, mapf)
		// fmt.Println(reply)

	} else { // reduce task

	}

}

func mapWork(task Task, mapf func(string, string) []KeyValue) {

	filename := task.FileName
	file, err := os.Open(filename)
	intermediate := []KeyValue{}
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

	outputfiles := make([]*os.File, task.Nreduce)
	encoders := make([]*json.Encoder, task.Nreduce)
	prefix := strings.Split(filename, ".txt")[0]

	for i:=0;i<task.Nreduce;i++{
		tempFileName := prefix + "_" + strconv.Itoa(i)
		outputfiles[i], err = os.Create("./mr-tmp/"+tempFileName)
		encoders[i] = json.NewEncoder(outputfiles[i])
	}


	for _, kv := range intermediate {
		index := ihash(kv.Key)%task.Nreduce
		encoders[index].Encode(kv)
	}



	for i := 0; i < task.Nreduce; i++ {
		outputfiles[i].Close()
	}
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
