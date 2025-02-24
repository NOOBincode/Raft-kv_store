package raft

import (
	"context"
	"errors"
	"raft-kv_store/pkg/util"
	"sync"
	"time"
)

const rpcTimeOut = time.Duration(200) * time.Millisecond
const rpcSnapshotTimeout = rpcTimeOut * 3

var errNoLongerLeader = errors.New("该节点不再是领导者")

func (n *node) enterLeaderState() {
	n.NodeState = NodeStateLeader
	n.currentLeader = n.nodeID

	//重置所有跟随者的indicies
	n.peerMgr.resetFollowerIndicies(n.logMgr.LastIndex())

	//发送心跳
	n.sendHeartbeat()
	util.WriteInfo("T%d: node%d赢得选举\n", n.currentTerm, n.NodeID())
}

func (n *node) sendHeartbeat() {
	n.peerMgr.tryReplicateAll()
	n.refreshTimer()
}

func (n *node) replicateData(follower *Peer) int {
	doReplicate := n.prepareReplication(follower)
	reply, err := doReplicate()
	if err != nil {
		util.WriteTrace("T%d:向该node:%d复制data失败.%s", n.currentTerm, follower.NodeID, err)
		reply = nil
	}
	return n.processReplicationResult(follower, reply)
}

func (n *node) processReplicationResult(follower *Peer, reply *AppendEntriesReply) int {
	n.mu.Lock()
	defer n.mu.Unlock()

	if reply == nil {
		return follower.matchIndex
	}

	if follower.NodeID != n.nodeID {
		util.Panicf("AppendEntry 请求所需的nodeID %d 不同,期望:%d", reply.NodeID, follower.NodeID)
	}

	if n.tryFollowNewTerm(reply.LeaderID, reply.Term, false) {
		return -1
	}

	if n.NodeState != NodeStateLeader {
		return -1
	}
	//更新跟随者的日志索引
	follower.updateMatchIndex(reply.Success, reply.LastMatch)
	//检查这里有没有需要提交的日志
	newCommit := reply.Success && n.leaderCommit()

	if newCommit || !follower.upToDate(n.logMgr.LastIndex()) {
		follower.tryRequestReplicateTo(nil)
	}
	return follower.matchIndex
}

func (n *node) leaderCommit() bool {
	commitIndex := n.logMgr.CommitIndex()
	for i := n.logMgr.LastIndex(); i > n.logMgr.CommitIndex(); i-- {
		entry := n.logMgr.GetLogEntry(i)

		if entry.Term < n.currentTerm {
			//不允许提交过去任期的日志,一个领导者只能他自己提交日志,任期内才可以
			break
		} else if entry.Term > n.currentTerm {
			util.Panicln("条目的任期大于当前领导者的任期")
			continue
		}
		if n.peerMgr.quorumReached(i) {
			commitIndex = i
			break
		} //大多数到达了i的状态,则需要更新上次提交日志的索引到当前位置
	}
	if commitIndex > n.logMgr.CommitIndex() {
		util.WriteTrace("T%d:领导者%d已经同步到大多数跟随者L%d中", n.currentTerm, n.nodeID, commitIndex)
		return true
	}
	return false
}

func (n *node) prepareReplication(follower *Peer) func() (*AppendEntriesReply, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.NodeState != NodeStateLeader {
		return func() (*AppendEntriesReply, error) { return nil, errNoLongerLeader }
	}
	currentTerm := n.currentTerm

	//需要快照的场景
	if follower.shouldSendSnapshot(n.logMgr.SnapshotIndex()) {
		req := n.createSnapshotRequest()
		return func() (*AppendEntriesReply, error) {
			ctx, cancel := context.WithTimeout(context.Background(), rpcSnapshotTimeout)
			defer cancel()

			util.WriteTrace("T%d:向node:%d(T%dL%d)发送快照中", currentTerm, follower.NodeID, req.SnapshotTerm, req.SnapshotIndex)
			return follower.InstallSnapshot(ctx, req)
		}
	}
	nextIndex, entryCount := follower.getReplicationParams()
	req := n.createAERequest(nextIndex, entryCount)
	return func() (*AppendEntriesReply, error) {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeOut)
		defer cancel()

		util.WriteVerbose("T%d:发送AppendEntry请求到Node %d.prevIndex: %d,entryCnt: %d", currentTerm, follower.NodeID, nextIndex, entryCount)
		return follower.AppendEntries(ctx, req)
	}
}

func (n *node) createSnapshotRequest() *SnapshotRequest {
	return &SnapshotRequest{
		SnapshotRequestHeader: SnapshotRequestHeader{
			Term:          n.currentTerm,
			LeaderID:      n.nodeID,
			SnapshotIndex: n.logMgr.SnapshotIndex(),
			SnapshotTerm:  n.logMgr.SnapshotTerm(),
		},
		File: n.logMgr.SnapshotFile(),
	}
}

// createAERequest 使用一个正确的日志负载创建一个 AppendEntriesRequest
func (n *node) createAERequest(startIdx, maxCnt int) *AppendEntriesRequest {
	//确保startIdx要比快照的index小,endIdx要比lastIndex小
	startIdx = max(startIdx, n.logMgr.SnapshotIndex()+1)
	endIdx := min(n.logMgr.LastIndex()+1, startIdx+maxCnt)

	//获取日志条目并且复制一份
	logs, prevIdx, prevTerm := n.logMgr.GetLogEntries(startIdx, endIdx)
	entries := make([]LogEntry, len(logs))
	copy(entries, logs)

	req := &AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.nodeID,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.logMgr.CommitIndex(),
	}
	return req
}

func (n *node) tryFollowNewTerm(sourceNodeID, newTerm int, isAppendEntries bool) bool {
	follow := false
	if newTerm > n.currentTerm {
		util.WriteInfo("T%d:从Node%d,获取了新任期,newTerm:%d.\n", n.currentTerm, sourceNodeID, newTerm)
		follow = true
	} else if newTerm == n.currentTerm && isAppendEntries {
		follow = true
	}
	if follow {
		n.enterFollowerState(sourceNodeID, newTerm)
	}
	return follow
}
func (n *node) enterFollowerState(sourceNodeID, NewTerm int) {
	oldLeader := n.currentLeader
	n.NodeState = NodeStateFollower
	n.currentLeader = sourceNodeID
	n.setTerm(NewTerm)

	//重置计时器
	n.refreshTimer()

	if n.nodeID != sourceNodeID && oldLeader != n.currentLeader {
		util.WriteInfo("T%d:Node%d 在新任期跟随 Node%d\n", n.currentTerm, n.nodeID, sourceNodeID)
	}
}

func (n *node) leaderExecute(ctx context.Context, cmd *StateMachineCmd) (*ExecuteReply, error) {
	n.mu.Lock()
	targetIndex := n.logMgr.ProcessCmd(*cmd, n.currentTerm)
	n.mu.Unlock()

	//尝试复制新条目到所有跟随者上
	n.peerMgr.waitAll(func(peer *Peer, wg *sync.WaitGroup) {
		peer.requestReplicateTo(targetIndex, wg)
	})
	success := n.logMgr.CommitIndex() >= targetIndex
	return &ExecuteReply{NodeID: n.nodeID, Success: success}, nil
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
