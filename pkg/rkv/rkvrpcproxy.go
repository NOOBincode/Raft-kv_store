package rkv

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"raft-kv_store/pkg/raft"
	"raft-kv_store/pkg/rkv/pb"
	"raft-kv_store/pkg/util"
)

var errorInvalidGetRequest = errors.New("没有获取到key")

var errorInvalidExecuteRequest = errors.New("执行的请求不是set也不是delete")

type execFunc func(context.Context, *raft.StateMachineCmd) (*raft.ExecuteReply, error)

type rkvRPCProxy struct {
	executeMap map[int]execFunc
	rpcClient  pb.KVStoreRaftClient
}

func (proxy *rkvRPCProxy) AppendEntries(ctx context.Context, req *raft.AppendEntriesRequest) (reply *raft.AppendEntriesReply, err error) {
	if resp, err := proxy.rpcClient.AppendEntries(ctx, fromRaftAERequest(req)); err == nil {
		reply = toRaftAEReply(resp)
	}
	return
}

func (proxy *rkvRPCProxy) RequestVote(ctx context.Context, req *raft.RequestVoteRequest) (reply *raft.RequestVoteReply, err error) {
	rv := fromRaftRVRequest(req)
	if resp, err := proxy.rpcClient.RequestVote(ctx, rv); err == nil {
		reply = toRaftRVReply(resp)
	}
	return reply, err
}

func (proxy *rkvRPCProxy) InstallSnapshot(ctx context.Context, req *raft.SnapshotRequest) (*raft.AppendEntriesReply, error) {
	stream, err := proxy.rpcClient.InstallSnapshot(ctx)
	if err != nil {
		return nil, err
	}

	writer := raft.NewSnapshotStreamWriter(&req.SnapshotRequestHeader, func(header *raft.SnapshotRequestHeader, bytes []byte) error {
		snapshotRequest := fromRaftSnapshotRequestHeader(header)
		snapshotRequest.Data = bytes
		return stream.Send(snapshotRequest)
	})
	raft.SendSnapshot(req.File, writer)

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return nil, err
	}
	return toRaftAEReply(resp), nil
}

func (proxy *rkvRPCProxy) Get(ctx context.Context, req *raft.GetRequest) (*raft.GetReply, error) {
	if len(req.Params) != 1 {
		return nil, errorInvalidGetRequest
	}
	getRequest := fromRaftGetRequest(req)
	resp, err := proxy.rpcClient.Get(ctx, getRequest)
	if err != nil {
		return nil, err
	}
	return toRaftGetReply(resp), nil
}

func (proxy *rkvRPCProxy) Execute(ctx context.Context, cmd *raft.StateMachineCmd) (*raft.ExecuteReply, error) {
	handler, ok := proxy.executeMap[cmd.CmdType]
	if !ok {
		return nil, errorInvalidExecuteRequest
	}
	return handler(ctx, cmd)
}

var rkvProxyFactory = &rkvRPCProxy{}

func (proxy *rkvRPCProxy) NewPeerProxy(info raft.NodeInfo) raft.IPeerProxy {
	conn, err := grpc.NewClient(info.EndPoint, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	if err != nil {
		util.Panicln(err)
	}
	client := pb.NewKVStoreRaftClient(conn)

	newProxy := &rkvRPCProxy{
		executeMap: make(map[int]execFunc, 2),
		rpcClient:  client,
	}
	newProxy.executeMap[KVCmdSet] = newProxy.executeSet
	newProxy.executeMap[KVCmdDel] = newProxy.executeDel
	return newProxy
}

func (proxy *rkvRPCProxy) executeSet(ctx context.Context, cmd *raft.StateMachineCmd) (*raft.ExecuteReply, error) {
	if cmd.CmdType != KVCmdSet {
		util.Panicln("在执行Set指令时传入错误的指令")
	}

	req := fromRaftSetRequest(cmd)

	var resp *pb.SetReply
	var err error
	if resp, err = proxy.rpcClient.Set(ctx, req); err != nil {
		return nil, fmt.Errorf("错误的将Set指令代理给leader,%s", err)
	}
	return toRaftSetReply(resp), nil
}

func (proxy *rkvRPCProxy) executeDel(ctx context.Context, cmd *raft.StateMachineCmd) (*raft.ExecuteReply, error) {
	if cmd.CmdType != KVCmdDel {
		util.Panicln("在执行Del指令时传入错误的指令")
	}
	req := fromRaftDeleteRequest(cmd)
	var resp *pb.DeleteReply
	var err error
	if resp, err = proxy.rpcClient.Delete(ctx, req); err != nil {
		return nil, fmt.Errorf("错误的将Del指令代理给leader,%s", err)
	}
	return toRaftDeleteReply(resp), nil
}
