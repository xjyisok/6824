package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

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
type Taskargs struct{}
type Task struct {
	Tasktype   TaskType
	TaskId     int
	Chunkslice []string
	Numreduce  int
}
type TaskType int

type Taskphase int

type State int

const (
	MapPhase Taskphase = iota
	ReducePhase
	DonePhase
)
const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask
	Exit
)

const (
	Working State = iota // 此阶段在工作
	Waiting              // 此阶段在等待执行
	Done                 // 此阶段已经做完
)

type Taskcell struct {
	Task      *Task
	TaskState State
	Starttime time.Time
}
type Taskholder struct {
	MetaMap map[int]*Taskcell
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
