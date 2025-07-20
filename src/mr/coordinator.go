package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var (
	mu sync.Mutex
)

type Coordinator struct {
	// Your definitions here.
	numreduce     int
	phase         Taskphase
	filelist      []string
	Mapchannel    chan *Task
	Reducechannel chan *Task
	Tasklist      Taskholder
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) Reply(args *Taskargs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.phase {
	case MapPhase:
		{
			if len(c.Mapchannel) > 0 {
				*reply = *<-c.Mapchannel
				c.Tasklist.MetaMap[reply.TaskId].TaskState = Working
				c.Tasklist.MetaMap[reply.TaskId].Starttime = time.Now()
			} else {
				reply.Tasktype = WaitingTask
				if c.checkphaseDone() {
					fmt.Println(c.phase)
					c.ToNextPhase()
				}
			}
			return nil
		}
	case ReducePhase:
		{
			fmt.Println("ReducePhase:")
			fmt.Println(time.Now())
			if len(c.Reducechannel) > 0 {
				*reply = *<-c.Reducechannel
				c.Tasklist.MetaMap[reply.TaskId+len(c.filelist)].TaskState = Working
				c.Tasklist.MetaMap[reply.TaskId+len(c.filelist)].Starttime = time.Now()
			} else {
				reply.Tasktype = WaitingTask
				if c.checkphaseDone() {
					c.ToNextPhase()
				}
			}
			return nil
		}
	case DonePhase:
		{
			fmt.Println("Exit:")
			reply.Tasktype = Exit
		}
	}
	return nil
}
func (c *Coordinator) ReportDone(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.Tasktype {
	case MapTask:
		{
			if c.Tasklist.MetaMap[args.TaskId].TaskState == Working {
				c.Tasklist.MetaMap[args.TaskId].TaskState = Done
			}
			break
		}
	case ReduceTask:
		{
			if c.Tasklist.MetaMap[args.TaskId+len(c.filelist)].TaskState == Working {
				c.Tasklist.MetaMap[args.TaskId+len(c.filelist)].TaskState = Done
			}
			break
		}
	}
	return nil
}
func (c *Coordinator) ToNextPhase() {
	if c.phase == MapPhase {
		c.makeReduceTask()
		c.phase = ReducePhase
		fmt.Println("ReducePhase started")
		fmt.Println(c.phase)
	} else if c.phase == ReducePhase {
		c.phase = DonePhase
	}
}
func (c *Coordinator) makeReduceTask() {
	for i := 0; i < c.numreduce; i++ {
		task := Task{
			Tasktype:  ReduceTask,
			TaskId:    i,
			Numreduce: c.numreduce,
		}
		taskcell := Taskcell{
			Task:      &task,
			TaskState: Waiting,
		}
		if c.Tasklist.MetaMap[i+len(c.filelist)] == nil {
			c.Tasklist.MetaMap[i+len(c.filelist)] = &taskcell
		}
		c.Reducechannel <- &task
	}
}
func (c *Coordinator) makeMapTask() {
	i := 0
	for _, v := range c.filelist {
		task := Task{
			Tasktype:   MapTask,
			TaskId:     i,
			Chunkslice: []string{v},
			Numreduce:  c.numreduce,
		}
		taskcell := Taskcell{
			Task:      &task,
			TaskState: Waiting,
		}
		if c.Tasklist.MetaMap[i] == nil {
			c.Tasklist.MetaMap[i] = &taskcell
		}
		c.Mapchannel <- &task
		i++
	}
}
func (c *Coordinator) checkphaseDone() bool {
	var (
		mapdone    = 0
		reducedone = 0
	)
	for _, v := range c.Tasklist.MetaMap {
		if v.Task.Tasktype == MapTask {
			if v.TaskState == Done {
				mapdone++
			}
		} else if v.Task.Tasktype == ReduceTask {
			if v.TaskState == Done {
				reducedone++
			}
		}
	}
	if mapdone == len(c.filelist) && c.phase == MapPhase {
		fmt.Println("MapPhase exit")
		return true
	}
	if reducedone == c.numreduce {
		fmt.Println("ReducePhase exit")
		return true
	}
	return false
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	if c.phase == DonePhase {
		//fmt.Printf("All tasks are finished,the coordinator will be exit! !")
		return true
	} else {
		return false
	}
}
func (c *Coordinator) CrashDetector() {
	for {
		mu.Lock()
		//time.Sleep(time.Second * 1)
		if c.phase == DonePhase {
			mu.Unlock()
			break
		} else {
			for _, v := range c.Tasklist.MetaMap {
				if v.TaskState == Working && time.Since(v.Starttime) >= 10*time.Second {
					if v.Task.Tasktype == MapTask {
						//c.Mapchannel <- v.Task
						c.Tasklist.MetaMap[v.Task.TaskId].TaskState = Waiting
						c.Mapchannel <- v.Task
					} else {
						//c.Reducechannel <- v.Task
						c.Tasklist.MetaMap[v.Task.TaskId+len(c.filelist)].TaskState = Waiting
						c.Reducechannel <- v.Task
					}
				}
			}
		}
		mu.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		numreduce:     nReduce,
		phase:         MapPhase,
		filelist:      files,
		Mapchannel:    make(chan *Task, len(files)),
		Reducechannel: make(chan *Task, nReduce),
		Tasklist: Taskholder{
			MetaMap: make(map[int]*Taskcell, len(files)+nReduce), // 任务的总数应该是files + Reducer的数量
		},
	}
	c.makeMapTask()
	c.server()
	// Your code here.
	go c.CrashDetector()
	return &c
}
