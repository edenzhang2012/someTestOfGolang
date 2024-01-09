package consistenthashing

import (
	"crypto/sha256"
	"fmt"
	"sync"
)

const (
	defaultVnodeCap = 10 //G
)

type ConsistentHashingRing struct {
	rwLock sync.RWMutex
	Nodes  map[string]Node //key is node name
	Vnodes []Vnode
}

type Node struct {
	Name   string
	Cap    int64
	Vnodes map[int64]Vnode //key is vnode hash
}

type Vnode struct {
}

func NewConsistentHashingRing() *ConsistentHashingRing {
	return &ConsistentHashingRing{Nodes: make(map[string]Node)}
}

func (chr *ConsistentHashingRing) AddNode(name string, cap int64) error {
	//检查节点是否已经存在
	if _, ok := chr.Nodes[name]; ok {
		return fmt.Errorf("%s already exist", name)
	}

	// node := Node{
	// 	Name:   name,
	// 	Cap:    cap,
	// 	Vnodes: make(map[int64]Vnode),
	// }

	//生成hash，并添加到hash环
	nrVnodes := int(cap / (defaultVnodeCap * 1024 * 1024 * 1024))
	for i := 0; i < nrVnodes; i++ {
		vnodeName := fmt.Sprintf("%s-%05d", name, i)
		hash := sha256.New()
		hash.Write([]byte(vnodeName))
	}

	return nil
}
