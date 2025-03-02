package main

import (
	"flag"
	"fmt"
	"os"
	"raft-kv_store/pkg/raft"
	"raft-kv_store/pkg/rkv"
	"raft-kv_store/pkg/util"
	"strconv"
	"strings"
)

func main() {

	util.InitLogger(
		util.LevelVerbose,
		false,
		"/var/log/rkv.log",
	)
	nodeID := -1
	addresses := ""
	flag.IntVar(&nodeID, "nodeid", -1, "当前node id，0-total")
	flag.StringVar(&addresses, "addresses", "", "comma-separated list of addresses")
	flag.Parse()

	addrArray := strings.Split(addresses, ",")
	if nodeID < 0 || nodeID >= len(addrArray) {
		fmt.Println("NodeID 超出地址范围")
		printUsage()
		os.Exit(1)
	}

	port, err := getNodePort(nodeID, addrArray)
	if err != nil {
		fmt.Println(err)
		printUsage()
		os.Exit(1)
	}
	runPRC(nodeID, port, addrArray)
}

func printUsage() {
	fmt.Println("rkv -nodeid id -addresses node0address:port,node1address:port,node2addresses:port... -loglevel level")
	fmt.Println("   -id: 0 based current node ID, indexed into addresses to get local port")
	fmt.Println("   -addresses: comma separated server:port for all nodes")
}

func runPRC(nodeID int, port string, addresses []string) {
	//初始化同伴
	peers := make(map[int]raft.NodeInfo)
	for i, v := range addresses {
		if i == nodeID {
			continue
		}

		peers[i] = raft.NodeInfo{
			NodeID:   i,
			EndPoint: v,
		}
	}
	rkv.StartRKV(nodeID, port, peers)
	util.WriteInfo("启动RKV成功!")
}

func getNodePort(nodeID int, addresses []string) (string, error) {
	address := addresses[nodeID]

	parts := strings.SplitN(address, ":", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("node %d的地址（%s）无效，应该是 server:port", nodeID, address)
	}

	port := parts[1]
	_, err := strconv.Atoi(port)
	if err != nil {
		return "", fmt.Errorf("node %d address(%s) 有一个无效端口 ", nodeID, address)
	}
	return port, nil
}
