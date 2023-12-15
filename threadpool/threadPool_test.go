package threadpool_test

import (
	"fmt"
	"math/rand"
	"sync"
	"test/threadpool"
	"testing"
	"time"
)

func TestInitAndStop(t *testing.T) {
	poll, err := threadpool.NewThreadPool()
	if err != nil {
		t.Fatalf("create thread pool falied with %v", err)
	}
	t.Logf("poll: %s", poll.Describe())
	poll.Stop()

	poll, err = threadpool.NewThreadPool(6)
	if err != nil {
		t.Fatalf("create thread pool falied with %v", err)
	}
	t.Logf("poll: %s", poll.Describe())
	poll.Stop()

	poll, err = threadpool.NewThreadPool(10)
	if err != nil {
		t.Fatalf("create thread pool falied with %v", err)
	}
	t.Logf("poll: %s", poll.Describe())
	poll.Stop()

	poll, err = threadpool.NewThreadPool(65)
	if err != nil {
		t.Fatalf("create thread pool falied with %v", err)
	}
	t.Logf("poll: %s", poll.Describe())
	poll.Stop()

	poll, err = threadpool.NewThreadPool(1, 6)
	if err != nil {
		t.Fatalf("create thread pool falied with %v", err)
	}
	t.Logf("poll: %s", poll.Describe())
	poll.Stop()

	_, err = threadpool.NewThreadPool(6, 1)
	if err == nil {
		t.Fatalf("create thread want err but not")
	} else {
		t.Logf("err is %v", err)
	}
}

type Job struct {
	Idx  int
	Lock sync.Mutex
	Num  *int
}

func (job *Job) Worker() {
	microSecond := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(3)
	job.Lock.Lock()
	(*job.Num)++
	fmt.Println(job.Idx, " is working, num is ", *(job.Num), " sleep is ", microSecond)
	job.Lock.Unlock()
	time.Sleep(time.Duration(microSecond) * time.Second)
}
func TestAddJob(t *testing.T) {
	poll, err := threadpool.NewThreadPool()
	if err != nil {
		t.Fatalf("create thread pool falied with %v", err)
	}
	defer poll.Stop()

	num := 0
	for i := 0; i < 70; i++ {
		if err := poll.Add(&Job{Idx: i, Num: &num}); err != nil {
			fmt.Println("Add failed with ", err)
		}
	}

}
