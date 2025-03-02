package rkv

import (
	"os"
	"raft-kv_store/pkg/raft"
	"raft-kv_store/pkg/util"
	"sync"
)

func StartRKV(nodeID int, port string, peers map[int]raft.NodeInfo) {
	cwd, err := os.Getwd()
	if err != nil {
		util.Panicf("获取当前工作中的快照目录失败.%s", err)
	}

	raft.SetSnapshotPath(cwd)

	//创建节点node
	node, err := raft.NewNode(nodeID, peers, newRkvStore(), rkvProxyFactory)
	if err != nil {
		util.Fatalf("%s", err)
	}
	//创建 rpc server
	var wg sync.WaitGroup
	rpcServer := newRKVRPCServer(node, &wg)

	//启动
	rpcServer.Start(port)
	node.Start()
	wg.Wait()
}
