package mr

// 定义worker

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

type ByKey []KeyValue

// 绑定排序相关的方法
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyMsg string
type ValueMsg struct {
	OperateTime time.Time
	Money       decimal.Decimal
}
type KeyValue struct {
	Key   KeyMsg   `json:"key"`
	Value ValueMsg `json:"value"`
}

type WorkerMsg struct {
	WorkerId int
}

func (km KeyMsg) toString() string {
	return string(km)
}

// 计算hash值
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// map 任务
func mapJob(mapf func(string, string) []KeyValue) {
	workerMsg := &WorkerMsg{
		WorkerId: int(time.Now().UnixNano()),
	}
	partitionCache := make(map[int][]KeyValue, 0)
	for {
		resp := GetMapJob(workerMsg.WorkerId, MapJob)
		if resp.Type == MapJobDone {
			break
		}
		if resp.Type == CreateFiles {
			// 写到文件中
			fileNameList := make([]string, 0)
			if len(partitionCache) > 0 {
				for i := 0; i < resp.NReduce; i++ {
					oname := fmt.Sprintf("tmp/mr-local-%d-%d.json", workerMsg.WorkerId, i)
					ofile, err := os.Create(oname)
					if err != nil {
						log.Println(err)
					}
					jsonContent, err := json.Marshal(partitionCache[i])
					if err != nil {
						log.Fatalf("json.Marshal() ")
					}
					fmt.Fprintf(ofile, "%v", string(jsonContent))
					ofile.Close()
					fileNameList = append(fileNameList, oname)
				}
				if len(fileNameList) > 0 {
					//   可以一次性确认,失败新文件名不会被记录
					finishMapJob(workerMsg.WorkerId, fileNameList)
				}
				partitionCache = make(map[int][]KeyValue, 0)
			}

			//发送信号时要保证->被接收 || 可能导致追加了但是超时了 (可以换 workedID!!)
			workerMsg = &WorkerMsg{
				WorkerId: int(time.Now().UnixNano()),
			}
			fmt.Println("更换WorkerId.............")
			time.Sleep(1000 * time.Millisecond)
			continue
		}
		fileName := resp.FileNameList[0]
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}

		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", fileName)
		}
		file.Close()
		kvs := mapf(fileName, string(content))
		for _, kv := range kvs {
			index := ihash(kv.Key.toString()) % resp.NReduce
			partitionCache[index] = append(partitionCache[index], kv)
		}
	}
}

// reduce 任务
func reduceJob(reducef func(KeyMsg, []ValueMsg) string) {
	for {
		intermediate := []KeyValue{}
		resp := GetReduceJob(ReduceJob)
		id := resp.Num
		if resp.Type == ReduceJobDone {
			// 所有reduce job 结束了
			break
		}
		if resp.Type == SleepType {
			time.Sleep(20 * time.Millisecond)
			continue
		}

		for _, fileName := range resp.FileNameList {
			if exist := strings.Contains(fileName, fmt.Sprint("-", id, ".json")); exist {
				file, err := os.Open(fileName)
				if err != nil {
					log.Fatalf("cannot open %v", fileName)
				}
				kvs := []KeyValue{}
				jsonContent, err := ioutil.ReadAll(file)
				if err := json.Unmarshal(jsonContent, &kvs); err != nil {
					log.Fatalf("cannot read %v", fileName)
				}
				for _, kv := range kvs {
					if ihash(kv.Key.toString())%resp.NReduce == resp.Num {
						intermediate = append(intermediate, KeyValue{
							Key:   kv.Key,
							Value: kv.Value,
						})
					}
				}
				file.Close()
			}
		}
		sort.Sort(ByKey(intermediate))
		oname := fmt.Sprintf("tmp/mr-out-%d", resp.Num)
		ofile, _ := os.Create(oname)
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []ValueMsg{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
			i = j
		}
		ofile.Close()
		finishReduceJob(oname, resp.Num)
	}
}

// Worker
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(KeyMsg, []ValueMsg) string,
) {
	mapJob(mapf)
	reduceJob(reducef)
}

// 获取map任务
func GetMapJob(workerId int, jobType JobType) *SendJobResponse {
	req := &GetJobRequest{
		WorkerId: workerId,
		Type:     jobType,
	}
	resp := &SendJobResponse{}
	call("Coordinator.SendJob", req, resp)
	return resp
}

// 获取reduce任务
func GetReduceJob(jobType JobType) *SendJobResponse {
	req := &GetJobRequest{
		Type: jobType,
	}
	resp := &SendJobResponse{}
	call("Coordinator.SendJob", req, resp)
	return resp
}

// 完成map任务时调用
func finishMapJob(workerId int, fileName []string) {
	notify := &FinishMapJobNotify{
		WorkerId:     workerId,
		FileNameList: fileName,
	}
	call("Coordinator.DoneMapJob", notify, &Empty{})
	log.Printf("finish job notify --> workerId: %v\n", notify.WorkerId)
}

// 完成reduce任务时调用
func finishReduceJob(fileName string, reduceNum int) {
	notify := &FinishReduceJobNotify{
		ReduceNum: reduceNum,
		FileName:  fileName,
	}
	call("Coordinator.DoneReduceJob", notify, &Empty{})
	log.Printf("finish job notify --> ReduceNum: %v\n", notify.ReduceNum)
}

// RPC 请求
func call(rpcname string, args interface{}, reply interface{}) bool {
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

	log.Println(err)
	return false
}
