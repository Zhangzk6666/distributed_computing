package mr

// 定义 rpc

import (
	"os"
	"strconv"
)

type JobType int

const (
	MapJob        JobType = 1
	ReduceJob     JobType = 2
	MapJobDone    JobType = 3
	ReduceJobDone JobType = 4
	Done          JobType = 5
	CreateFiles   JobType = 6
	SleepType     JobType = 7
)

type GetJobRequest struct {
	Type     JobType
	WorkerId int
}

type SendJobResponse struct {
	Type         JobType
	FileNameList []string
	Num          int
	NReduce      int
}

type FinishMapJobNotify struct {
	WorkerId     int
	FileNameList []string
}

type FinishReduceJobNotify struct {
	ReduceNum int
	FileName  string
}
type Empty struct {
}

type DoneStatus struct {
	IsDone bool
}

// 在/var/tmp中为coordinator创建一个惟一的unix域套接字名称。
func coordinatorSock() string {
	s := "/var/tmp/distributed_computing-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
