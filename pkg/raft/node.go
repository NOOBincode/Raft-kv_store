package raft

import (
	"context"
	"errors"
	"pkg/util"
	"sync"
)

var errorInsufficientPeers = errors.New("至少需要2个以上的同伴")
var errorCurrentNodeInPeers = errors.New("当前节点不应该作为他的同伴存在")
var errorInvalidPeerNodeID = errors.New("同伴节点需要一个有效的ID")
var errorNoLeaderAvailable = errors.New("当前没有有效的领导者")

type NodeState int

const (
	// NodeStateFollower 表示跟随者的状态
	NodeStateFollower = NodeState(1)
	// NodeStateCandidate 表示候选人状态
	NodeStateCandidate = NodeState(2)
	// NodeStateLeader 表示领导者状态
	NodeStateLeader = NodeState(3)
)

// NodeInfo 保存每一个同伴节点的信息和它的IP地址
type NodeInfo struct {
	NodeID   int
	EndPoint string
}

type INodeRPCProvider interface {
	// AppendEntries 压入一个条目
	AppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesReply, error)
	// RequestVote 发起投票请求
	RequestVote(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error)
	// InstallSnapshot 下载一个快照
	InstallSnapshot(ctx context.Context, req *SnapshotRequest) (*AppendEntriesReply, error)
	// Get 从状态机中获取已提交和被使用的值
	Get(ctx context.Context, req *GetRequest) (*GetReply, error)
	// Execute 执行一个写操作
	Execute(ctx context.Context, cmd *StateMachineCmd) (*ExecuteReply, error)
}

// INode 表示一个raft节点
type INode interface {
	// Start 启动该节点
	Start()

	// Stop 停止该节点
	Stop()

	// NodeID 返回节点的ID
	NodeID() int

	// OnSnapshot
	OnSnapshot(part *SnapshotRequestHeader) bool

	INodeRPCProvider
}

type node struct {
	mu sync.RWMutex

	clusterSize   int
	nodeID        int
	NodeState     NodeState
	currentTerm   int
	currentLeader int
	votedFor      int
	votes         map[int]bool
	logMgr        IlogManager
	peerMgr       peerManager
	timer         IRaftTimer
}

func NewNode(nodeID int, peers map[int]NodeInfo, sm IStateMachine, proxyFactory IPeerProxyFactory) (INode, error) {
	if err := validateCluster(nodeID, peers); err != nil {
		return nil, err
	}
	size := len(peers) + 1

	n := &node{
		mu:            sync.RWMutex{},
		clusterSize:   size,
		nodeID:        nodeID,
		NodeState:     NodeStateFollower,
		votedFor:      -1,
		currentTerm:   0,
		currentLeader: -1,
		votes:         make(map[int]bool, size),
		logMgr:        newLogMgr(nodeID, sm),
	}
	n.timer = newRaftTimer(n.onTimer)
	n.peerMgr = newPeerManager(peers,,proxyFactory)
}

func validateCluster(nodeID int, peers map[int]NodeInfo) error {
	if len(peers) < 2 {
		return errorInsufficientPeers
	}

	for i, p := range peers {
		if p.NodeID != nodeID {
			return errorCurrentNodeInPeers
		} else if p.NodeID != i {
			return errorInvalidPeerNodeID
		}
	}
	return nil
}

func (n *node) NodeID() int {
	return n.nodeID
}

func (n *node) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.timer.stop()
	n.peerMgr.stop()
}

func (n *node) Get(ctx context.Context, req *GetRequest) (result *GetReply, err error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	var ret interface{}
	if ret, err = n.logMgr.Get(req.Params...); err != nil {
		return
	}

	result = &GetReply{
		NodeID: n.nodeID,
		Data:   ret,
	}

	return
}
func (n *node) onTimer(state NodeState, term int) {
	n.mu.RLock()
	var fn func()
	if state == n.NodeState && term == n.currentTerm {
		if n.NodeState == NodeStateLeader {
			fn = n.sendHeartbeat
		}else{
			fn = n.
		}
	}
	n.mu.RUnlock()
	if fn != nil {
		fn()
	}

}

func (n *node) enterCandidateState() {
	n.NodeState = NodeStateCandidate
	n.currentLeader = -1
	n.setTerm(n.currentTerm + 1)

	//先为自己投票
	n.votedFor = n.nodeID
	n.votes = make(map[int]bool,n.clusterSize)
	n.votes[n.nodeID] = true

	//重置计时器
	n.refreshTimer()
}

func (n *node) setTerm(newTerm int) {
	if newTerm < n.currentTerm {
		util.Panicf("不能设置新任期因为新任期短于当前任期")
	}
	if newTerm > n.currentTerm {
		//在更长任期上重置投票信息
		n.votedFor = -1
	}
	n.currentTerm = newTerm
}

func (n *node) refreshTimer() {
	n.timer.reset(n.NodeState,n.currentTerm)
}

func (n *node) startElection(){
	runCampaign := n.pre
}

func (n *node) prepareCampaign() func() <-chan *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.enterCandidateState()

	req := &RequestVoteRequest{
		Term:         n.currentTerm,
		CandidateID:  n.nodeID,
		LastLogIndex: n.logMgr.LastIndex(),
		LastLogTerm:  n.logMgr.LastTerm(),
	}
	currentTerm := n.currentTerm

	return func() <-chan *RequestVoteReply {
		
	}
}