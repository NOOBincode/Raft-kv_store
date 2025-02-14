package raft

import (
	"math/rand"
	"pkg/util"
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

		}
	}
}

var firstFollow = true

func getTimeout(state NodeState, term int) (timeout time.Duration) {
	if state == NodeStateFollower {
		timeout = heartbeatTimeout
		return
	}

	ms := rand.Intn(heartbeatTimeoutMS-heartbeatTimeoutMS) + heartbeatTimeoutMS
	timeout = time.Duration(ms) * time.Millisecond

	isFollow := term > 0 && state == NodeStateFollower
	if firstFollow && isFollow {
		timeout = timeout * 10
		firstFollow = false
	}
	return
}
