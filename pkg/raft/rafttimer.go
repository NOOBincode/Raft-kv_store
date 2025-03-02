package raft

import (
	"math/rand"
	"raft-kv_store/pkg/util"
	"sync"
	"time"
)

const minElectionTimeoutMS = 600
const maxElectionTimeoutMS = 2000
const heartbeatTimeoutMS = 150
const heartbeatTimeout = time.Duration(heartbeatTimeoutMS) * time.Millisecond

type IRaftTimer interface {
	start()
	stop()
	reset(newState NodeState, term int)
}

type resetEvt struct {
	state NodeState
	term  int
}

type raftTimer struct {
	wg       sync.WaitGroup
	timer    *time.Timer
	evtChan  chan resetEvt
	callback func(state NodeState, term int)
}

func newRaftTimer(timerCallback func(state NodeState, term int)) *raftTimer {
	rt := &raftTimer{
		evtChan:  make(chan resetEvt, 100),
		callback: timerCallback,
	}
	return rt
}

func (rt *raftTimer) stop() {
	util.StopTimer(rt.timer)
	rt.wg.Wait()
	rt.timer = nil
}

func (rt *raftTimer) start() {
	rt.timer = time.NewTimer(time.Hour * 24)
	rt.wg.Add(1)
	util.WriteInfo("启动Timer中..")
	go rt.run()
}

func (rt *raftTimer) reset(newState NodeState, term int) {
	rt.evtChan <- resetEvt{newState, term}
}

func (rt *raftTimer) run() {
	state, term := NodeStateFollower, 0
	stop := false
	for !stop {
		select {
		case evt := <-rt.evtChan:
			state, term = evt.state, evt.term
			timeout := getTimeout(state, term)
			util.WriteInfo("%d", timeout)
			util.WriteVerbose("重置计时器,state:%d,term:%d,timeout:%d", state, term, timeout)
			util.RestTimer(rt.timer, timeout)
		case _, ok := <-rt.timer.C:
			if !ok {
				stop = true
				break
			}
			util.WriteVerbose("计时事件结束,state:%d,term:%d,timeout:%d\", state, term, timeout")
			rt.callback(state, term)
		}
	}
	rt.wg.Done()
}

var firstFollow = true

func getTimeout(state NodeState, term int) (timeout time.Duration) {
	if state == NodeStateFollower {
		timeout = heartbeatTimeout
		return
	}

	ms := rand.Intn(maxElectionTimeoutMS-minElectionTimeoutMS) + minElectionTimeoutMS
	timeout = time.Duration(ms) * time.Millisecond

	isFollow := term > 0 && state == NodeStateFollower
	if firstFollow && isFollow {
		//节点开始时投票,根据收到的推举信息来确定当前集群的任期,但是leader建立RPC连接需要时间,心跳会延迟,如果太快开始新投票,会导致当前集群增加任期,同时其他节点开始选举
		//最坏的情况是leader和其他节点都照常工作但是出现选举风暴
		//缓解方法就是让跟随者下一次选举超时时间适当延长
		timeout = timeout * 10
		firstFollow = false
	}
	return
}
