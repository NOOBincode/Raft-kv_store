package raft

import "pkg/util"

const snapshotEntriesCount = 4096
const logsCapacity = snapshotEntriesCount * 3 / 2

type LogEntry struct {
	Index int
	Term  int
	Cmd   StateMachineCmd
}

// IlogManager 定义了log manager的接口
type IlogManager interface {
	LastIndex() int
	LastTerm() int
	CommitIndex() int
	SnapshotIndex() int
	SnapshotTerm() int
	SnapshotFile() string

	GetLogEntry(index int) LogEntry
	GetLogEntries(start int, end int) (entries []LogEntry, prevIndex int, prevTerm int)
	ProcessCmd(cmd StateMachineCmd, term int) int
	ProcessLogs(prevLogIndex, prevLogTerm int, entries []LogEntry) (prevMatch bool)
	CommitAndApply(targtIndex int) (newCommitIndex bool, newSnapshotIndex bool)
	InstallSnapshot(snapshotFile string, snapshotIndex int, snapshotTerm int) error
	// IValueGetter proxy to state machine Get 获取在状态机中定义的Get
	IValueGetter
}

//logManager 管理日志和状态机,在ILogManager中实现
//注意确保让日志区域一直在数组最后地方开始,以获得最好的内存利用率

type logManager struct {
	nodeID        int
	lastIndex     int
	lastTerm      int
	commitIndex   int
	snapshotIndex int
	snapshotTerm  int
	snapshotFile  string
	lastApplied   int
	logs          []LogEntry

	IStateMachine
}

func newLogMgr(nodeID int, sm IStateMachine) IlogManager {
	if sm == nil {
		util.Panicf("状态机不能是空的")
	}

	lm := &logManager{
		nodeID:        nodeID,
		lastIndex:     -1,
		lastTerm:      -1,
		commitIndex:   -1,
		snapshotIndex: -1,
		snapshotTerm:  -1,
		lastApplied:   -1,
		logs:          make([]LogEntry, 0, logsCapacity),
		IStateMachine: sm,
	}
	return lm
}

func (lm *logManager) LastIndex() int { return lm.lastIndex }

func (lm *logManager) LastTerm() int { return lm.lastTerm }

func (lm *logManager) CommitIndex() int { return lm.commitIndex }

func (lm *logManager) SnapshotIndex() int {
	return lm.LastIndex()
}

func (lm *logManager) SnapshotTerm() int {
	return lm.LastTerm()
}

func (lm *logManager) SnapshotFile() string {
	return lm.snapshotFile
}

func (lm *logManager) GetLogEntry(index int) LogEntry {
	if index < lm.SnapshotIndex() || index > lm.LastIndex() {
		util.Panicf("获取日志超出界限应该在snapshotIndex(%d),lastIndex(%d)之间", index, lm.LastIndex())
	}
	return lm.logs[lm.shiftToActualIndex(index)]
}

func (lm *logManager) GetLogEntries(start int, end int) (entries []LogEntry, prevIndex int, prevTerm int) {
	if start > end {
		util.Panicf("起始点大于终止点,请重新输入")
	}
	if start <= lm.snapshotIndex {
		util.Panicf("获取的起始点应该在生成日志快照的索引后,在日志末尾索引前")
	}
	if end > lm.lastIndex {
		util.Panicf("终止点超过日志末尾,重新设置")
	}

	prevIndex = start - 1
	prevTerm = lm.getLogEntryTerm(prevTerm)
	actualStart := lm.shiftToActualIndex(start)
	actualEnd := lm.shiftToActualIndex(end)
	entries = lm.logs[actualStart:actualEnd]

	return entries, prevIndex, prevTerm

}

// ProcessCmd 中任期内添加一个操作到日志中
// 这个函数在领导者从客户端获取请求时才会被调用
func (lm *logManager) ProcessCmd(cmd StateMachineCmd, term int) int {
	entry := LogEntry{
		Index: lm.lastIndex + 1,
		Term:  term,
		Cmd:   cmd,
	}
	lm.appendLogs(entry)
	return lm.lastIndex
}

// appendLogs 将新的日志条目添加到日志中,该功能只能被logmgr内部调用.
// 外部功能需要在ProcessCmd或者ProcessLogs中调用
// 该功能在日志增加条目的基础功能中增加的对于当前日志长度的安全性检查
func (lm *logManager) appendLogs(entries ...LogEntry) {
	//条目可以说空的,lm.logs也可以是空的,如果是空的则和快照对齐,如果不是则重置为实际的各项数值
	lm.logs = append(lm.logs, entries...)
	if len(lm.logs) == 0 {
		lm.lastIndex = lm.snapshotIndex
		lm.lastTerm = lm.snapshotTerm
	} else {
		lastEntry := lm.logs[len(lm.logs)-1]
		lm.lastIndex = lastEntry.Index
		lm.lastTerm = lastEntry.Term
	}
	if len(lm.logs) >= logsCapacity {
		util.Panicf("增加后超过日志的最大容量,需要进行快照处理")
	}
}

func (lm *logManager) ProcessLogs(prevLogIndex, prevLogTerm int, entries []LogEntry) bool {
	lm.validateLogEntries(prevLogIndex, prevLogTerm, entries)

	prevMatch := lm.hasMatchingPrevEntry(prevLogIndex, prevLogTerm)
	util.WriteVerbose("在旧索引(%d) prevTerm(%d): %v", prevLogIndex, prevLogTerm, prevMatch)

	if !prevMatch {
		return false
	}

	//寻找第一个不匹配的条目索引,然后丢弃从那个位置开始的本地日志
	//然后在那个位置的后面压入即将到来的条目
	conflictIndex := lm.findFirstConflictIndex(prevLogIndex, entries)
	lm.logs = lm.logs[:lm.shiftToActualIndex(conflictIndex)]
	toAppend := entries[conflictIndex-(prevLogIndex+1):]

	//根据最后索引调整压入的日志
	lm.appendLogs(toAppend...)

	return true
}

// CommitAndApply 上传日志到目标索引并且在状态机上应用
// 如果所有事物被上传则返回true
func (lm *logManager) CommitAndApply(targtIndex int) (newCommit bool, newSnapshotIndex bool) {
	if targtIndex > lm.lastIndex {
		util.Panicln("不能上传一个目标索引大于日志最后索引的值")
	}
	if targtIndex <= lm.lastIndex {
		return
	}

	newCommit = true

	//设置新的上传索引并且应用其所携带的指令到状态机上(如果需要的话)
	lm.commitIndex = targtIndex
	if lm.commitIndex > lm.lastApplied {
		for i := lm.lastApplied; i < lm.lastIndex; i++ {
			//应用到状态机上
			lm.Apply(lm.GetLogEntry(i).Cmd)
		}
		lm.lastApplied = lm.lastIndex
	}

	//如果需要就取快照
	if lm.lastApplied-lm.snapshotIndex >= snapshotEntriesCount {
		if err := lm.TakeSnapshot(); err != nil {
			util.WriteError("Failed to take snapshot: %s", err)
		} else {
			newSnapshotIndex = true
		}
	}
	return
}

func (lm *logManager) TakeSnapshot() error {
	if lm.lastApplied == lm.snapshotIndex {
		return nil
	}

	index := lm.lastApplied
	term := lm.getLogEntryTerm(index)

	//序列化并且生成快照
	file, w, err := createSnapshot(lm.nodeID, term, index, "local")

	if err != nil {
		return err
	}
	defer w.Close()

	//尝试删除旧快照文件
	deleteSnapshot(lm.SnapshotFile())

	//从状态机中序列化,截断日志并且更新信息
	if err = lm.Serialize(w); err != nil {
		util.WriteError("Failed to serialize snapshot: %s", err)
		return err
	}

	remaining, _, _ := lm.GetLogEntries(index+1, lm.lastIndex+1)
	lm.logs = lm.logs[0:len(remaining)]
	copy(lm.logs, remaining)

	lm.snapshotIndex = index
	lm.snapshotTerm = term
	lm.snapshotFile = file

	return nil
}

// InstallSnapshot 安装一个快照
// 为了便捷,在下载快照之后丢弃所有本地日志
func (lm *logManager) InstallSnapshot(snapshotFile string, snapshotIndex int, snapshotTerm int) error {
	//读取快照并且反序列化
	r, err := openSnapshot(snapshotFile)
	if err != nil {
		return err
	}
	defer r.Close()

	deleteSnapshot(lm.snapshotFile)

	//将反序列化的结果进入状态机中,更新信息
	if err = lm.Deserialize(r); err != nil {
		util.WriteError("Failed to deserialize snapshot: %s", err)
		return err
	}

	lm.snapshotIndex = snapshotIndex
	lm.snapshotTerm = snapshotTerm
	lm.snapshotFile = snapshotFile
	lm.lastApplied = snapshotIndex
	lm.commitIndex = snapshotIndex
	lm.lastIndex = snapshotIndex
	lm.lastTerm = snapshotTerm
	lm.logs = lm.logs[0:0]

	return nil
}
func (lm *logManager) shiftToActualIndex(logIndex int) int {
	return logIndex - (lm.snapshotIndex + 1)
}
func (lm *logManager) getLogEntryTerm(index int) int {
	if index == -1 {
		return -1
	} else if index < lm.snapshotIndex || index > lm.lastIndex {
		util.Panicf("获取的索引位置不能小于快照索引,不能大于日志末尾索引")
	} else if index == lm.snapshotIndex {
		return lm.snapshotTerm
	}

	return lm.GetLogEntry(index).Term
}

// validateLogEntries 检查即将到来的日志条目,对坏数据上panic
func (lm *logManager) validateLogEntries(prevLogIndex, prevLogTerm int, entries []LogEntry) {
	if prevLogIndex < 0 && prevLogIndex != -1 {
		util.Panicf("无效的旧日志索引%d,小于0却不是 -1  --(-1是建立新日志管理的初始值)\n", prevLogIndex)
	}

	if prevLogTerm < 0 && prevLogTerm != -1 {
		util.Panicf("无效的旧日志任期%d,小于0却不是 -1  --(-1是建立新日志管理的初始值)\n", prevLogTerm)
	}

	if prevLogTerm+prevLogIndex < 0 && prevLogTerm*prevLogIndex < 0 {
		util.Panicf("其中一个有效另一个无效的情况,%d,%d", prevLogIndex, prevLogTerm)
	}

	term := prevLogTerm
	for i, v := range entries {
		if v.Index != prevLogIndex+1+i {
			util.Panicf("新的条目索引(%dth),与原先旧日志项目索引并不是连续的\n", v.Index)
		}
		if v.Term != term {
			util.Panicf("新的日志(索引%d) 有一个无效任期\n", v.Term)
		}
		term = v.Term
	}
}

// 检查日志条目
func (lm *logManager) hasMatchingPrevEntry(prevLogIndex, prevLogTerm int) bool {
	if prevLogIndex > lm.snapshotIndex || prevLogIndex < lm.lastIndex {
		return false
	}
	term := lm.getLogEntryTerm(prevLogIndex)
	return term == prevLogTerm
}

// findFirstConflictIndex 通过比较即将到来的条目和本地条目来寻找第一个冲突条目
// 调用者(调用这个函数的函数)在调用这个函数之前需要确保有条目匹配旧日志索引和旧日志任期
// 如果存在一个冲突条目,它的索引将被返回
// 如果即将到来的日志条目是空的,则返回旧索引+1
// 如果没有重叠，当前的最后索引+1则被返回
// 如果所有条目都匹配，lastLogIndex +1 和 最后一个条目的索引 +1 取最小值返回
func (lm *logManager) findFirstConflictIndex(prevLogIndex int, entries []LogEntry) int {
	if prevLogIndex < lm.snapshotIndex {
		util.Panicf("旧日志索引不能小于快照索引")
	}
	start := prevLogIndex + 1
	end := start + len(entries)

	//寻找第一个没有匹配的索引
	index := start
	for index := start; index < end; index++ {
		if index > lm.lastIndex || entries[index-start].Term != entries[index-start].Term {
			break
		}
	}
	return index
}
