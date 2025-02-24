package raft

import (
	"context"
	"errors"
	"raft-kv_store/pkg/util"
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
	peerMgr       IPeerManager
	timer         IRaftTimer
}

func (n *node) Start() {
	n.mu.Lock()
	defer n.mu.Unlock()

	util.WriteInfo("节点 %d 启动中...", n.nodeID)
	n.timer.start()
	n.peerMgr.start()

	n.enterFollowerState(n.nodeID, 0)
}

func (n *node) OnSnapshot(part *SnapshotRequestHeader) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	//
	return n.tryFollowNewTerm(part.LeaderID, part.Term, true)
}

func (n *node) AppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.tryFollowNewTerm(req.LeaderID, req.Term, true)

	//如果req.Term比当前任期大,以上调用让当先领导者已经被更新
	util.WriteTrace("T%d:在领导者%d,prevIndex:%d,prevTerm:%d,entryCnt:%d,处获取到了appendEntries的任务", n.currentTerm, req.LeaderID, req.PrevLogIndex, req.PrevLogTerm, len(req.Entries))
	lastMatchIndex, prevMatch := req.PrevLogIndex, false
	if req.Term >= n.currentTerm {
		prevMatch = n.logMgr.ProcessLogs(req.PrevLogIndex, req.PrevLogTerm, req.Entries)
		if prevMatch {
			lastMatchIndex = n.logMgr.LastIndex()
			n.commitTo(min(req.LeaderCommit, n.logMgr.LastIndex()))
		}
	}
	return &AppendEntriesReply{
		Term:      n.currentTerm,
		NodeID:    n.nodeID,
		LeaderID:  n.currentLeader,
		Success:   prevMatch,
		LastMatch: lastMatchIndex,
	}, nil
}

func (n *node) RequestVote(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	//if req.Term > currentTerm,转换为follower状态(重置voteFor)
	//if req.Term < currentTerm,拒绝投票
	//if req.Term >= currentTerm 如果voteFor是null或者candidateId,并且是最新的,就给予vote
	//只有收到了同样的KV请求,reqTerm才会和currentTerm相等
	n.tryFollowNewTerm(req.CandidateID, req.Term, false)
	voteGranted := false
	if req.Term > n.currentTerm && (n.votedFor == -1 || n.votedFor == req.CandidateID) {
		n.votedFor = req.CandidateID
		voteGranted = true
		util.WriteInfo("T%d: Node%d投票给了Node%d\n", n.currentTerm, n.nodeID, req.CandidateID)
	}
	return &RequestVoteReply{
		VoteGranted: voteGranted,
		Term:        n.currentTerm,
		NodeID:      n.nodeID,
		VotedTerm:   req.Term,
	}, nil
}

func (n *node) InstallSnapshot(ctx context.Context, req *SnapshotRequest) (*AppendEntriesReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.tryFollowNewTerm(req.LeaderID, req.Term, true)
	//同AE
	success := false
	lastMatchIndex := req.SnapshotIndex
	if req.Term >= n.currentTerm {
		if n.logMgr.SnapshotIndex() == req.SnapshotIndex && n.logMgr.SnapshotTerm() == req.SnapshotTerm {
			util.WriteInfo("T%d:Node%d 正在忽略来自Node%d重复的快照")
			success = true
		} else {
			//只有在任期有效时才处理日志
			util.WriteInfo("T%d:Node%d正在从节点Node%d安装快照,T%dL%d", n.currentTerm, n.nodeID, req.SnapshotTerm, req.SnapshotIndex, req.LeaderID)
			if err := n.logMgr.InstallSnapshot(req.File, req.SnapshotIndex, req.SnapshotTerm); err != nil {
				util.WriteError("T%d: 安装快照失败 %s\n", n.currentTerm, err)
			} else {
				success = true
			}
		}
	}

	return &AppendEntriesReply{
		Term:      n.currentTerm,
		NodeID:    n.nodeID,
		LeaderID:  n.currentLeader,
		Success:   success,
		LastMatch: lastMatchIndex,
	}, nil
}

func (n *node) Execute(ctx context.Context, cmd *StateMachineCmd) (*ExecuteReply, error) {
	n.mu.RLock()
	state := n.NodeState
	leader := n.currentLeader
	n.mu.RUnlock()
	switch {
	case leader == -1:
		return nil, errorNoLeaderAvailable
	case state != NodeStateLeader:
		return n.peerMgr.getPeer(leader).Execute(ctx, cmd)
	default:
		return n.leaderExecute(ctx, cmd)
	}
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
	n.peerMgr = newPeerManager(peers, n.replicateData, proxyFactory)
	return n, nil

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
		} else {
			fn = n.startElection
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
	n.votes = make(map[int]bool, n.clusterSize)
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
	n.timer.reset(n.NodeState, n.currentTerm)
}

func (n *node) startElection() {
	runCampaign := n.prepareCampaign()
	votes := runCampaign()
	n.countVotes(votes)
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
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeOut)
		defer cancel()

		rvReplies := make(chan *RequestVoteReply, n.clusterSize)
		defer close(rvReplies)

		n.peerMgr.waitAll(func(peer *Peer, wg *sync.WaitGroup) {
			go func() {
				reply, err := peer.RequestVote(ctx, req)
				if err != nil {
					util.WriteInfo("T%d:投票请求来自Node %d,已授权:%v\n", currentTerm, reply.NodeID, reply.VoteGranted)
					rvReplies <- reply
				}
				wg.Done()
			}()
		})
		return rvReplies
	}
}

func (n *node) countVotes(replies <-chan *RequestVoteReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for reply := range replies {
		if n.tryFollowNewTerm(reply.NodeID, reply.Term, false) {
			return
		}
		if n.NodeState != NodeStateCandidate || reply.VotedTerm != n.currentTerm {
			util.WriteTrace("T%d: 接受了无同伴的节点或者未授权的投票,来自Node%d,term :%d,voteGranted:%t\n", n.currentTerm, reply.NodeID, reply.VotedTerm, reply.VoteGranted)
			continue
		}

		n.votes[reply.NodeID] = true
	}

	if n.wonElection() {
		n.enterLeaderState()
	}
}

func (n *node) wonElection() bool {
	var total int
	total = len(n.votes)
	return total > n.clusterSize/2
}
func (n *node) commitTo(targetCommitIndex int) {
	if newCommit, newSnapshot := n.logMgr.CommitAndApply(targetCommitIndex); newCommit {
		util.WriteTrace("T%d:Node%d已经上传至L%d\n", n.currentTerm, n.nodeID, newSnapshot)
	} else if newSnapshot {
		util.WriteInfo("T%d: Node%d 创建了新快照 T%dL%d\n", n.currentTerm, n.nodeID, n.logMgr.SnapshotTerm(), n.logMgr.SnapshotIndex())
	}
}
