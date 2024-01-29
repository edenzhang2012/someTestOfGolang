package mongodb

import (
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/mgo.v2/bson"
)

type Task struct {
	//Recursive bool   `bson:"recursive"`
	TaskId                     string   `bson:"task_id"`                                 //标识任务的唯一的id，唯一索引
	TaskType                   string   `bson:"task_type"`                               //任务类型，索引
	PrimaryLogicalPath         string   `bson:"primary_logical_path"`                    //主操作逻辑路径，索引
	PrimaryClusterId           string   `bson:"primary_cluster_id"`                      //主操作的目的集群（数据准备、副本同步、孤本数据迁移时，为写的集群），删除整个数据集时不用该参数，索引
	SecondaryLogicalPath       string   `bson:"secondary_logical_path,omitempty"`        //配合的逻辑路径，索引
	SecondaryClusterId         string   `bson:"secondary_cluster_id,omitempty"`          //配合的目的集群（数据准备、副本同步、孤本数据迁移时，为读的集群），索引
	Priority                   int      `bson:"priority"`                                //任务优先级
	Files                      []string `bson:"files,omitempty"`                         //复制部分数据到一个数据集时使用
	Suffix                     string   `bson:"suffix,omitempty"`                        //删除的子目录路径(删除子目录时用到)，索引
	CloneType                  int      `bson:"clone_type,omitempty"`                    //克隆类型 1：从用户空间克隆到社区； 2：从社区克隆到用户空间 3:代码克隆
	RealPath                   string   `bson:"real_path"`                               //实际路径，快照相关
	Status                     string   `bson:"status"`                                  //任务的状态，索引
	ErrorMessage               string   `bson:"error_message,omitempty"`                 //任务执行中的错误信息
	ErrorCode                  int      `bson:"error_code,omitempty"`                    //任务执行中的错误码
	ConflictInfo               string   `bson:"conflict_info,omitempty"`                 //因为其他任务冲突导致的当前任务失败时，填充冲突的信息，由public-gemini里constant.go里ConflictInfo序列化出来
	RetryCount                 int      `bson:"retry_count"`                             //重试次数，不能为空，第一次插入时应该为1。 每次retry的时候，不重新选择集群。
	PavoAgentTaskId            string   `bson:"pavo_agent_task_id"`                      //请求到pavo-agent执行的任务的id， 可以为空，注意：多个task可能会对应同一个pavo-agent-task,比如不同taskid的对相同的数据集的准备，数据离线同步和在线同步处理相同的数据集到相同的集群。索引
	Progress                   string   `bson:"progress,omitempty"`                      //进度，可以为空，跨集群传数据时的进度，需要定时查该进度。
	RealSize                   int64    `bson:"real_size,omitempty"`                     //真实大小，可以为空，记录计算大小任务返回的大小
	LogicalSize                int64    `bson:"logical_size,omitempty"`                  //逻辑大小，可以为空，记录计算大小任务返回的大小
	NotifyUpstreamStatus       string   `bson:"notify_upstream_status"`                  //通知上游的状态， 必须有值
	NotifyUpstreamCount        int      `bson:"notify_upstream_count,omitempty"`         //通知上游的次数，可以为0，通知后有值
	NotifyUpstreamErrorMessage string   `bson:"notify_upstream_error_message,omitempty"` //通知上游返回的错误信息，可以为空，通知且返回报错后有值
	ZipType                    int      `bson:"zip_type,omitempty"`                      //解压类型
	UnzipPath                  string   `bson:"unzip_path,omitempty"`                    //解压需求里的解压路径，相对路径（相对当前数据集的路径）
	//FixOrGcTime                time.Time      `bson:"fix_or_gc_time,omitempty"`                //fix或者GC时间
	Cancel          bool      `bson:"cancel,omitempty"`            //被取消
	CancelPavoAgent bool      `bson:"cancel_pavo_agent,omitempty"` //取消pavo-agent任务
	CreateTime      time.Time `bson:"create_time"`                 //创建时间  //创建索引
	UpdateTime      time.Time `bson:"update_time"`                 //更新时间
	Expire          time.Time `bson:"expire"`                      //超时时间
}

func (m *Task) TableName() string {
	return "task"
}

func (m *Task) GetClearClusterId() string {
	return m.PrimaryClusterId
}

func (m *Task) Insert() error {
	if m.CreateTime.IsZero() {
		m.CreateTime = time.Now()
	}
	if m.UpdateTime.IsZero() {
		m.UpdateTime = m.CreateTime
	}
	if err := Insert("pavostor", m.TableName(), m); err != nil {
		fmt.Println("insert task error", "error", err.Error(), m.TableName(), m)
		return err
	}
	fmt.Println("insert task success", m.TableName(), m, "task", *m)
	return nil
}

func (m *Task) QueryOne() error {
	query := bson.M{}
	if m.TaskId != "" {
		query["task_id"] = m.TaskId
	}
	if m.TaskType != "" {
		query["task_type"] = m.TaskType
	}
	if m.PrimaryLogicalPath != "" {
		query["primary_logical_path"] = m.PrimaryLogicalPath
	}
	if m.PrimaryClusterId != "" {
		query["primary_cluster_id"] = m.PrimaryClusterId
	}
	if m.SecondaryLogicalPath != "" {
		query["secondary_logical_path"] = m.SecondaryLogicalPath
	}
	if m.SecondaryClusterId != "" {
		query["secondary_cluster_id"] = m.SecondaryClusterId
	}
	if m.Priority > 0 {
		query["priority"] = m.Priority
	}
	if m.Suffix != "" {
		query["suffix"] = m.Suffix
	}
	if m.CloneType > 0 {
		query["clone_type"] = m.CloneType
	}
	if m.RealPath != "" {
		query["real_path"] = m.RealPath
	}
	if m.PavoAgentTaskId != "" {
		query["pavo_agent_task_id"] = m.PavoAgentTaskId
	}
	if m.NotifyUpstreamStatus != "" {
		query["notify_upstream_status"] = m.NotifyUpstreamStatus
	}
	if m.Status != "" {
		query["status"] = m.Status
	}

	if err := FindOne("pavostor", m.TableName(), query, nil, m); err != nil {
		if err == mongo.ErrNoDocuments {
			fmt.Println("query task error not found", "query", query)
			return err
		}
		fmt.Println("query task error", "error", err, "query", query)
		return err
	}

	fmt.Println("query task success", "result", m, "query", query)
	return nil
}

// 修改task，但不设置为终态
func (m *Task) Update() error {
	//查询条件
	selector := bson.M{}
	if m.TaskId != "" {
		selector["task_id"] = m.TaskId
	} else {
		fmt.Println("task_id can not be empty", "task", m)
		return errors.New("task_id cannot be empty")
	}

	update := bson.M{}
	if !m.Expire.IsZero() {
		update["expire"] = m.Expire
	}
	if m.TaskType != "" {
		update["task_type"] = m.TaskType
	}
	if m.PrimaryLogicalPath != "" {
		update["primary_logical_path"] = m.PrimaryLogicalPath
	}
	if m.PrimaryClusterId != "" {
		update["primary_cluster_id"] = m.PrimaryClusterId
	}
	if m.SecondaryLogicalPath != "" {
		update["secondary_logical_path"] = m.SecondaryLogicalPath
	}
	if m.SecondaryClusterId != "" {
		update["secondary_cluster_id"] = m.SecondaryClusterId
	}
	if m.Priority > 0 {
		update["priority"] = m.Priority
	}
	if m.Suffix != "" {
		update["suffix"] = m.Suffix
	}
	if len(m.Files) > 0 {
		update["files"] = m.Files
	}
	if m.CloneType > 0 {
		update["clone_type"] = m.CloneType
	}
	if m.RealPath != "" {
		update["real_path"] = m.RealPath
	}
	if m.Status != "" {
		update["status"] = m.Status
	}
	if m.ErrorMessage != "" {
		update["error_message"] = m.ErrorMessage
	}
	if m.ErrorCode > 0 {
		update["error_code"] = m.ErrorCode
	}
	if m.RetryCount > 0 {
		update["retry_count"] = m.RetryCount
	}
	if m.PavoAgentTaskId != "" {
		update["pavo_agent_task_id"] = m.PavoAgentTaskId
	}
	if m.Progress != "" {
		update["progress"] = m.Progress
	}
	if m.RealSize > 0 {
		update["real_size"] = m.RealSize
	}
	if m.LogicalSize > 0 {
		update["logical_size"] = m.LogicalSize
	}
	if m.NotifyUpstreamStatus != "" {
		update["notify_upstream_status"] = m.NotifyUpstreamStatus
	}
	if m.NotifyUpstreamCount > 0 {
		update["notify_upstream_count"] = m.NotifyUpstreamCount
	}
	if m.NotifyUpstreamErrorMessage != "" {
		update["notify_upstream_error_message"] = m.NotifyUpstreamErrorMessage
	}
	if m.UnzipPath != "" {
		update["unzip_path"] = m.UnzipPath
	}
	if m.ZipType != 0 {
		update["zip_type"] = m.ZipType
	}
	if m.Cancel {
		update["cancel"] = m.Cancel
	}
	if m.CancelPavoAgent {
		update["cancel_pavo_agent"] = m.CancelPavoAgent
	}
	//if !m.FixOrGcTime.IsZero() {
	//	update["fix_or_gc_time"] = m.FixOrGcTime
	//}
	update["update_time"] = time.Now()

	if err := Update("pavostor", m.TableName(), selector, bson.M{"$set": update}); err != nil {
		fmt.Println("update task error", "error", err, "selector", selector, "update", update)
		return err
	}
	return nil
}

func (m *Task) QueryAll(query bson.M) ([]*Task, error) {
	if query == nil {
		query = bson.M{}
	}
	var result []*Task
	if err := FindAll("pavostor", m.TableName(), query, nil, &result); err != nil {
		fmt.Println("query all task error", "error", err, "query", query)
		return result, err
	}

	if len(result) == 0 {
		return result, mongo.ErrNoDocuments
	}

	//fmt.Println("query all task success", "result", &result, "query", query)
	return result, nil
}
func (m *Task) Delete() error {
	if m.TaskId == "" {
		fmt.Println("delete task task_id cannot be empty", "task", m)
		return errors.New("task_id invalid")
	}
	if err := Remove("pavostor", m.TableName(), bson.M{"task_id": m.TaskId}); err != nil {
		if err == mongo.ErrNoDocuments {
			fmt.Println("delete task error already does not exist", "task_id", m.TaskId)
			return nil
		}
		fmt.Println("delete task error", "error", err.Error(), "task_id", m.TaskId)
		return err
	}
	fmt.Println("delete task success", "task_id", m.TaskId)
	return nil
}
