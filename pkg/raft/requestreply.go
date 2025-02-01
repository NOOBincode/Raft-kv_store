package raft

// AppendEntriesRequest 压入条目请求
type AppendEntriesRequest struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply 压入条目的响应
type AppendEntriesReply struct {
	NodeID    int
	Term      int
	LeaderID  int
	LastMatch int
	Success   bool
}

// RequestVoteRequest 关于投票的请求
type RequestVoteRequest struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// 关于投票的响应
type RequestVoteReply struct {
	NodeID      int
	Term        int
	VotedTerm   int
	VoteGranted bool
}

type SnapshotRequestHeader struct {
	Term          int
	LeaderID      int
	SnapshotIndex int
	SnapshotTerm  int
}

type SnapshotRequest struct {
	SnapshotRequestHeader
	File string
}

type GetRequest struct {
	Params []interface{}
}

type GetReply struct {
	NodeID int
	Data   interface{}
}

type ExecuteReply struct {
	NodeID  int
	Success bool
}
