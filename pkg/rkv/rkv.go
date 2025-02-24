package rkv

import (
	"os"
	"raft-kv_store/pkg/raft"
	"raft-kv_store/pkg/util"
)

func StartRKV(nodeID int, port string, peers map[int]raft.NodeInfo) {
	cwd, err := os.Getwd()
	if err != nil {
		util.Panicf("获取当前工作中的快照目录失败.%s", err)
	}

	raft.SetSnapshotPath(cwd)

}
