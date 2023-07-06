package mr

// 定义coordinator

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

// 超时问题
type MapRecord struct {
	timeout     time.Time
	recordFiles []string
}
type ReduceRecord struct {
	timeout time.Time
}

type WorkerId int
type ReduceNum int
type Coordinator struct {
	mapJobFilesName []string
	// 如果发生错误,则需要恢复对应的文件名即可.
	// 成功则记录最终生成的reduce文件名
	mapJobRecord       map[WorkerId]*MapRecord
	reduceJobFilesName []string
	reduceIdList       []ReduceNum
	//如果发生错误,则需要恢复对应的文件名即可.
	//成功则记录最终生成的reduce文件名
	reduceJobRecord map[ReduceNum]*ReduceRecord
	nReduce         int
	resultFiles     []string
	mu              sync.Mutex
	isMapJobDone    bool
	isReduceJobDone bool
}

// 发放任务
func (c *Coordinator) SendJob(req *GetJobRequest, resp *SendJobResponse) error {
	for {
		c.mu.Lock()
		resp.NReduce = c.nReduce
		length := len(c.mapJobFilesName)
		if length > 0 {
			resp.Type = MapJob
			resp.FileNameList = append(resp.FileNameList, c.mapJobFilesName[length-1])
			c.mapJobFilesName = append(c.mapJobFilesName[:length-1])
			if _, exist := c.mapJobRecord[WorkerId(req.WorkerId)]; !exist {
				c.mapJobRecord[WorkerId(req.WorkerId)] = &MapRecord{
					timeout:     time.Now().Add(13 * time.Second),
					recordFiles: make([]string, 0),
				}
			}
			c.mapJobRecord[WorkerId(req.WorkerId)].timeout = time.Now().Add(13 * time.Second)
			c.mapJobRecord[WorkerId(req.WorkerId)].recordFiles = append(
				c.mapJobRecord[WorkerId(req.WorkerId)].recordFiles, resp.FileNameList...)
			fmt.Println(resp)
			c.mu.Unlock()
			return nil
		} else {
			if c.isMapJobDone {
				if req.Type == MapJob {
					resp.Type = MapJobDone
					fmt.Println("response type --->  MapJobDone")
					c.mu.Unlock()
					return nil
				}
				c.mu.Unlock()
				break
			} else {
				resp.Type = CreateFiles
				fmt.Println("CreateFiles")
				fmt.Println("c.isMapJobDone: ", c.isMapJobDone,
					"c.reduceJobRecord: ", c.reduceJobRecord)
				c.mu.Unlock()
				return nil
			}

		}
	}

	for {
		c.mu.Lock()
		resp.NReduce = c.nReduce
		resp.FileNameList = c.reduceJobFilesName

		length := len(c.reduceIdList)
		if length > 0 {
			resp.Type = ReduceJob
			resp.Num = int(c.reduceIdList[length-1])
			c.reduceIdList = append(c.reduceIdList[:length-1])

			if _, exist := c.reduceJobRecord[ReduceNum(resp.Num)]; !exist {
				c.reduceJobRecord[ReduceNum(resp.Num)] = &ReduceRecord{
					timeout: time.Now().Add(13 * time.Second),
				}
			}
			c.reduceJobRecord[ReduceNum(resp.Num)].timeout = time.Now().Add(13 * time.Second)
			c.mu.Unlock()
			return nil
		}

		if c.isReduceJobDone {
			if req.Type == ReduceJob {
				resp.Type = ReduceJobDone
			}
			c.mu.Unlock()
			return nil
		} else {
			resp.Type = SleepType
			c.mu.Unlock()
			return nil
		}
	}
}

// 统计map任务是否完成,完成时 `c.isMapJobDone = true`
func (c *Coordinator) DoneMapJob(req *FinishMapJobNotify, empty *Empty) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exist := c.mapJobRecord[WorkerId(req.WorkerId)]; exist {
		c.reduceJobFilesName = append(c.reduceJobFilesName, req.FileNameList...)
		delete(c.mapJobRecord, WorkerId(req.WorkerId))
		if len(c.mapJobFilesName) == 0 && len(c.mapJobRecord) == 0 {
			fmt.Println("all map job done")
			c.isMapJobDone = true
		}
	}
	return nil
}

// 统计reduce任务是否完成,完成时 `c.isReduceJobDone = true`
func (c *Coordinator) DoneReduceJob(req *FinishReduceJobNotify, empty *Empty) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exist := c.reduceJobRecord[ReduceNum(req.ReduceNum)]; exist {
		c.resultFiles = append(c.resultFiles, req.FileName)
		delete(c.reduceJobRecord, ReduceNum(req.ReduceNum))
		if len(c.reduceIdList) == 0 && len(c.reduceJobRecord) == 0 {
			fmt.Println("all reduce job done")
			c.isReduceJobDone = true
		}
	}
	return nil
}

// 启动RPC
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// 是否完成全部任务
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isReduceJobDone
}

// 创建 Coordinator
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		mapJobFilesName:    files,
		mapJobRecord:       make(map[WorkerId]*MapRecord),
		reduceJobFilesName: make([]string, 0),
		reduceIdList:       make([]ReduceNum, 0),
		reduceJobRecord:    make(map[ReduceNum]*ReduceRecord),
		nReduce:            nReduce,
		resultFiles:        make([]string, 0),
		mu:                 sync.Mutex{},
		isMapJobDone:       false,
		isReduceJobDone:    false,
	}
	for i := 0; i < nReduce; i++ {
		c.reduceIdList = append(c.reduceIdList, ReduceNum(i))
	}
	// map job超时检查
	go func(c *Coordinator) {
		for {
			c.mu.Lock()
			if len(c.mapJobRecord) != 0 {
				keys := make([]WorkerId, 0)
				for k, v := range c.mapJobRecord {
					if v.timeout.Before(time.Now()) {
						c.mapJobFilesName = append(c.mapJobFilesName, v.recordFiles...)
						keys = append(keys, k)
					}
				}
				for _, k := range keys {
					delete(c.mapJobRecord, k)
				}
			}
			c.mu.Unlock()
			time.Sleep(time.Second)
		}

	}(c)

	// reduce job超时检查
	go func(c *Coordinator) {
		for {
			c.mu.Lock()
			if len(c.reduceJobRecord) != 0 {
				keys := make([]ReduceNum, 0)
				for k, v := range c.reduceJobRecord {
					if v.timeout.Before(time.Now()) {
						c.reduceIdList = append(c.reduceIdList, k)
						keys = append(keys, k)
					}
				}
				for _, k := range keys {
					delete(c.reduceJobRecord, k)
				}
			}
			c.mu.Unlock()
			time.Sleep(time.Second)
		}

	}(c)
	c.server()
	return c
}
