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
	"strings"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	flag := true
	for flag {
		reply := Calltask()
		args := Task{}
		args.Tasktype = reply.Tasktype
		args.TaskId = reply.TaskId
		switch reply.Tasktype {
		case MapTask:
			{
				intermediate := []KeyValue{}
				filename := reply.Chunkslice[0]
				file, err := os.Open(filename)
				if err != nil {
					//log.Fatalf("cannot open %v", file)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					//log.Fatalf("cannot read %v", file)
				}
				intermediate = append(intermediate, mapf(filename, string(content))...)
				buckets := make(map[int][]KeyValue)
				for _, kv := range intermediate {
					rid := ihash(kv.Key) % reply.Numreduce
					buckets[rid] = append(buckets[rid], kv)
				}
				for i := 0; i < reply.Numreduce; i++ {
					oname := "mr-tmp-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(i)
					ofile, _ := os.Create(oname)
					enc := json.NewEncoder(ofile)
					for _, kv := range buckets[i] {
						err := enc.Encode(kv)
						if err != nil {
							return
						}
					}
					ofile.Close()
				}
				file.Close()
				ReportFinished(&args)
			}
		case ReduceTask:
			{
				s := []string{}
				path, _ := os.Getwd()
				files, _ := ioutil.ReadDir(path)
				for _, fi := range files {
					if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reply.TaskId)) {
						s = append(s, fi.Name())
					}
				}
				intermediate := shuffle(s)
				tempFile, err := ioutil.TempFile(path, "mr-tmp-*")
				if err != nil {
					log.Fatal("Failed to create temp file", err)
				}
				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					var values []string
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)
					fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
					i = j
				}
				tempFile.Close()

				// 在完全写入后进行重命名
				fn := fmt.Sprintf("mr-out-%d", reply.TaskId)
				os.Rename(tempFile.Name(), fn)
				ReportFinished(&args)
				fmt.Println(os.Getpid())
			}
		case WaitingTask:
			{
				fmt.Println("al task is running")
				//time.Sleep(time.Second * 1)
			}
		case Exit:
			{
				flag = false
			}
		}
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}
func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}

func Calltask() Task {
	args := Taskargs{}
	reply := Task{}
	ok := call("Coordinator.Reply", &args, &reply)
	if ok {
		//fmt.Println("worker get ", reply.Tasktype, "task :Id[", reply.TaskId, "]")
	} else {
		//fmt.Printf("call failed!\n")
	}
	return reply
}
func ReportFinished(args *Task) {
	r := Task{}
	ok := call("Coordinator.ReportDone", args, &r)
	if ok {
		//fmt.Println("worker finished signal received ")
	} else {
		//fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
