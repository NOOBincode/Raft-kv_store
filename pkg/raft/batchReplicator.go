package raft

import (
	"math"
	"pkg/util"
	"sync"
)

const targetAny = math.MaxInt

type replicationReq struct {
	targetID int
	reqwq    *sync.WaitGroup
}

// batchReplicator 尽最大努力去处理传入的请求,同时批量复制提升效率,根据ID和lastMatch来选择是否进行复制处理
type batchReplicator struct {
	replicateFn func() int
	requests    chan replicationReq
	wg          sync.WaitGroup
}

func newBatchReplicator(replicateFn func() int) *batchReplicator {
	return &batchReplicator{
		replicateFn: replicateFn,
		requests:    make(chan replicationReq, maxAppendEntriesCount),
	}
}

// start 启动批量处理流程
func (br *batchReplicator) start() {
	br.wg.Add(1)
	go func() {
		lastMatch := -1
		for req := range br.requests {
			if req.targetID > lastMatch {
				//启动新的批量处理操作，检查能否和targetID对接
				lastMatch = br.replicateFn()
			}

			if req.reqwq != nil {
				req.reqwq.Done()
			}
		}
		br.wg.Done()
	}()
}

// stop 停止批量复制处理器并且等待完成
func (br *batchReplicator) stop() {
	close(br.requests)
	br.wg.Wait()
}

// requestReplicateTo 根据targetID请求一个进程，当前请求队列满的时候堵塞
func (br *batchReplicator) requestReplicateTo(targetID int, wg *sync.WaitGroup) {
	if targetID < 0 || targetID == targetAny {
		util.Panicln("无效的targetID")
	}

	br.requests <- replicationReq{targetID: targetID, reqwq: wg}
}

func (br *batchReplicator) tryRequestReplicateTo(wg *sync.WaitGroup) {
	select {
	case br.requests <- replicationReq{targetID: targetAny, reqwq: wg}:
	default:
	}
}
