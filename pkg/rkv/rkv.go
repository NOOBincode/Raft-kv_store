package rkv

import (
	"os"
	"pkg/raft"
	"pkg/util"
)

func StartRKV(nodeID int, port string, peers map[int]raft.NodeInfo) {
	cwd, err := os.Getwd()
	if err != nil {
		util.Panicf("获取当前工作中的快照目录失败.%s", err)
	}

	raft.SetSnapshotPath(cwd)

}
