package main

import "raft-kv_store/pkg/util"

func main() {
	util.InitLogger(
		util.LevelVerbose,
		false,
		"/var/log/rkv.log",
	)
}
