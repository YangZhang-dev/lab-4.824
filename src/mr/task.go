package mr

type TaskType int8

const (
	TNoTask     TaskType = -1
	TMapTask    TaskType = 1
	TReduceTask TaskType = 2
)

type status int8

const (
	UN_ALLOCATION status = -1
	ALLOCATION    status = 1
	COMPLETE      status = 2
	TIMEOUT       status = 3
)

type Task struct {
	T              TaskType
	TargetFilePath string
	Status         status
	ID             int
	startTime      int64
}
type MapTask struct {
	Task
	SourceFilePath string
}
type ReduceTask struct {
	Task
	BuketKey     int
	BuketNumber  int
	FilePathList []string
}
