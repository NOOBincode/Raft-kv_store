package raft

import (
	"errors"
	"pkg/util"
	"sync"
)

var errorNoPeersProvided = errors.New("没有Raft同伴")
var errorInvalidNodeID = errors.New("无效的NodeID")

type IPeerProxy interface {
	INodeRPCProvider
}

type IPeerProxyFactory interface {
	NewPeerProxy(info NodeInfo) IPeerProxy
}

type IPeerManager interface {
	getPeer(nodeID int) *Peer
	waitAll(action func(*Peer, *sync.WaitGroup))
	resetFollowerIndicies(lastLogIndex int)
	quorumReached(lastLogIndex int) bool
	tryReplicateAll()

	start()
	stop()
}

// peerManager 管理同伴之间的沟通
type peerManager struct {
	peers map[int]*Peer
}

func (p *peerManager) getPeer(nodeID int) *Peer {
	peer, ok := p.peers[nodeID]
	if !ok {
		util.Panicln(errorInvalidNodeID)
	}
	return peer
}

func (p *peerManager) waitAll(action func(*Peer, *sync.WaitGroup)) {
	var wg sync.WaitGroup
	for _, peer := range p.peers {
		wg.Add(1)
		action(peer, &wg)
	}
	wg.Wait()
}

// resetFollowerIndicies 根据lastIndex重置跟随者的索引
func (p *peerManager) resetFollowerIndicies(lastLogIndex int) {
	for _, peer := range p.peers {
		peer.resetFollowIndex(lastLogIndex)
	}
}

// quorumReached 确定是否大部分的跟随者们同步了日志信息
func (p *peerManager) quorumReached(lastLogIndex int) bool {
	matchCnt := 1
	quorum := (len(p.peers) / 2) + 1
	for _, peer := range p.peers {
		if peer.hasConsensus(lastLogIndex) {
			matchCnt++
			if matchCnt > quorum {
				return true
			}
		}
	}
	return false
}

func (p *peerManager) tryReplicateAll() {
	for _, peer := range p.peers {
		peer.tryRequestReplicateTo(nil)
	}
}

func (p *peerManager) start() {
	for _, peer := range p.peers {
		peer.start()
	}
}

func (p *peerManager) stop() {
	for _, peer := range p.peers {
		peer.stop()
	}
}

// newPeerManager creates the node proxy for kv store
func newPeerManager(peers map[int]NodeInfo, replicate func(*Peer) int, proxyFactory IPeerProxyFactory) IPeerManager {
	if len(peers) == 0 {
		util.Panicf("%s\n", errorNoPeersProvided)
	}

	mgr := &peerManager{
		peers: make(map[int]*Peer),
	}

	for peerID, info := range peers {
		if peerID != info.NodeID {
			util.Panicf("同伴 %d 有不同的ID集合在 %d\n", peerID, info.NodeID)
		}

		peer := &Peer{
			NodeInfo:   info,
			nextIndex:  0,
			matchIndex: -1,
		}
		peer.IPeerProxy = proxyFactory.NewPeerProxy(info)
		peer.batchReplicator = newBatchReplicator(func() int { return replicate(peer) })
		mgr.peers[peerID] = peer
	}

	return mgr
}
