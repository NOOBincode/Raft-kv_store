package raft

import (
	"raft-kv_store/pkg/util"
)

const nextIndexFallbackStep = 1
const maxAppendEntriesCount = 64

type Peer struct {
	NodeInfo
	nextIndex  int
	matchIndex int

	*batchReplicator
	IPeerProxy
}

// hasMatch 找到同伴
func (p *Peer) hasMatch() bool {
	return p.matchIndex != -1
}

// getReplicationParams 获取下一个Index 和 entryCount 为了下一次复制
func (p *Peer) getReplicationParams() (nextIndex, entryCount int) {
	nextIndex = p.nextIndex
	entryCount = maxAppendEntriesCount
	if !p.hasMatch() {
		entryCount = -1
	}
	return nextIndex, entryCount
}

// 快照索引大于当前同伴的下一个索引，则需要快照了
func (p *Peer) shouldSendSnapshot(snapshotIndex int) bool {
	return snapshotIndex >= p.nextIndex
}

// 告知跟随者是否需要根据被给予的index进行更新
func (p *Peer) upToDate(lastIndex int) bool {
	return p.matchIndex >= lastIndex
}

// 检查当前同伴是否根据被给予的log index 在同一共识下
func (p *Peer) hasConsensus(logIndex int) bool {
	return p.matchIndex >= logIndex
}

func (p *Peer) resetFollowIndex(LastLogIndex int) {
	p.matchIndex = LastLogIndex + 1
	p.matchIndex = -1
}

// 更新node的匹配索引
func (p *Peer) updateMatchIndex(match bool, LastMatch int) {
	if match {
		if p.matchIndex < LastMatch {
			util.WriteVerbose("更新 Node %d 的nextIndex.lastMatch %d", p.NodeID, LastMatch)
			p.nextIndex = p.matchIndex + 1
			p.matchIndex = LastMatch
		} else {
			util.WriteVerbose("降低Node %d的 nextIndex.lastMatch %d", p.NodeID, LastMatch)
			p.nextIndex = max(0, p.nextIndex-nextIndexFallbackStep)
			p.matchIndex = -1
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
