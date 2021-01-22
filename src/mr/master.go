package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"plugin"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	taskLock    sync.Mutex
	taskQueue   []Task
	reduceQueue []Task
	reduceIndex int
}

type Task struct {
	TaskType     int //0 map, 1 reduce
	FileName     string
	Assigned     bool
	Completed    bool
	Nreduce      int
	AssignedTime time.Time
}

func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

type Task interface {
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (m *Master) CompleteMap(args ExampleArgs, reply *ExampleReply) {
	for index, value := range m.taskQueue {
		if value.FileName == args.FileName {
			m.taskQueue[index].Completed = true
			reduceTask := Task{1, value.FileName, false, false, m.reduceIndex, time.Now()}
			m.reduceQueue = append(m.reduceQueue, reduceTask)
			m.reduceIndex++
			break
		}
	}

}

func (m *Master) AssignTask(args *ExampleArgs, task *Task) error {
	m.taskLock.Lock()
	for i := range m.taskQueue {
		if m.taskQueue[i].Assigned == false {
			m.taskQueue[i].Assigned = true
			m.taskQueue[i].AssignedTime = time.Now()
			task.FileName = m.taskQueue[i].FileName
			task.Assigned = m.taskQueue[i].Assigned
			task.TaskType = m.taskQueue[i].TaskType
			task.Completed = m.taskQueue[i].Completed
			task.AssignedTime = m.taskQueue[i].AssignedTime
			task.Nreduce = m.taskQueue[i].Nreduce
			m.taskLock.Unlock()
			return nil
		}
	}
	for _, rdtask := range m.reduceQueue {

		if rdtask.Assigned == false {

			task.FileName = rdtask.FileName
			task.Assigned = rdtask.Assigned
			task.TaskType = rdtask.TaskType
			task.Completed = rdtask.Completed
			task.AssignedTime = rdtask.AssignedTime
			task.Nreduce = rdtask.Nreduce
			m.taskLock.Unlock()
			return nil
		}

	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
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
	m.reduceIndex = 0
	for _, value := range files {
		task := Task{0, value, false, false, nReduce, time.Now()}
		m.taskQueue = append(m.taskQueue, task)
	}

	m.server()
	return &m
}
