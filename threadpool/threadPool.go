package threadpool

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultMaxThreads       int32 = 64
	DefaultBeginningThreads int32 = 8
)

// var (
// 	logger *logrus.Logger
// )

// callers need implate this
type Job interface {
	/*
		用户的线程工作函数，具体工作内容用户自己定义，参数也由用户自己定义，错误需要用户自行处理
	*/
	Worker()
}

type ThreadPool struct {
	maxIdle    atomic.Int32   //当前空闲协程数量, 默认 8
	max        int32          //最大协程数量，默认64
	running    atomic.Int32   //当前正在工作的协程数量
	idle       atomic.Int32   //记录协程编号
	jobSignal  chan Job       //添加任务信号
	stopSignal chan struct{}  //停止线程池信号
	stop       bool           //停止状态
	growing    bool           //线程扩展中
	wg         sync.WaitGroup //等待所有协程退出
}

// func init() {
// 	logFile, err := os.OpenFile("./pool.log", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
// 	if err != nil {
// 		log.Fatalf("open file %s failed with %s\n", "./pool.log", err)
// 	}

// 	logger = logrus.New()
// 	logger.SetOutput(logFile)
// 	logger.SetLevel(logrus.InfoLevel)
// 	logger.SetReportCaller(true)
// }

func NewThreadPool(elem ...int32) (*ThreadPool, error) {
	tp := ThreadPool{}
	tp.jobSignal = make(chan Job)
	tp.stopSignal = make(chan struct{})
	args := len(elem)
	if args > 2 {
		return nil, errors.New("too many params")
	}

	if args == 0 {
		tp.maxIdle.Store(DefaultBeginningThreads)
		tp.max = DefaultMaxThreads
	} else if args == 1 {
		tp.maxIdle.Store(elem[0])
		if DefaultMaxThreads < tp.maxIdle.Load() {
			tp.max = tp.maxIdle.Load()
		} else {
			tp.max = DefaultMaxThreads
		}
	} else if args == 2 {
		if elem[1] < elem[0] {
			return nil, errors.New("bad params")
		}

		tp.maxIdle.Store(elem[0])
		tp.max = elem[1]
	}

	for i := int32(0); i < tp.maxIdle.Load(); i++ {
		go tp.thread()
	}

	return &tp, nil
}

func (tp *ThreadPool) Describe() string {
	return fmt.Sprintf("maxIdle %d, max %d, running %d", tp.maxIdle.Load(), tp.max, tp.running.Load())
}

func (tp *ThreadPool) Add(job Job) error {
	if tp.stop {
		return fmt.Errorf("thread poll is stoped")
	}

	for tp.growing {
		// logger.Infof("growing, wait")
		time.Sleep(500 * time.Millisecond)
	}

	running := tp.running.Load()
	maxIdle := tp.maxIdle.Load()
	if running == maxIdle {
		tp.growing = true
		end := int32(0)
		if 2*maxIdle > tp.max {
			end = tp.max
		} else {
			end = 2 * maxIdle
		}
		// logger.Infof("add to %d", end)

		for i := running; i < end; i++ {
			go tp.thread()
		}

		tp.maxIdle.CompareAndSwap(maxIdle, end)
		tp.growing = false
	}

	tp.jobSignal <- job
	return nil
}

func (tp *ThreadPool) Stop() {
	tp.stop = true
	//等待所有任务执行完成

	for tp.running.Load() != 0 {
		// logger.Infof("has %d running", tp.running.Load())
		time.Sleep(500 * time.Millisecond)
	}
	//所有协程退出
	for i := int32(0); i < tp.maxIdle.Load(); i++ {
		tp.stopSignal <- struct{}{}
	}

	tp.wg.Wait()
}

func (tp *ThreadPool) thread() {
	tp.idle.Add(1)
	// logger.Infof("thread %d begin to run", id)
	tp.wg.Add(1)
	for {
		select {
		case job := <-tp.jobSignal:
			tp.running.Add(1)
			// logger.Infof("%d begin to work, running %d", id, run)
			job.Worker()
			tp.running.Add(-1)
			// logger.Infof("%d work done, running %d", id, run)
		case <-tp.stopSignal:
			// logger.Infof("%d exit", id)
			tp.wg.Done()
			return
		}
	}
}
