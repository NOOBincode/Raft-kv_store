package raft

import (
	"context"
	"errors"
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
}
