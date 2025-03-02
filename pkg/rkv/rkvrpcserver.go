package rkv

import (
	"context"
	"google.golang.org/grpc"
	"net"
	"raft-kv_store/pkg/raft"
	"raft-kv_store/pkg/rkv/pb"
	"raft-kv_store/pkg/util"
	"sync"
	"time"
)

type rkvRPCServer struct {
	wg     *sync.WaitGroup
	node   raft.INode
	server *grpc.Server
	pb.UnimplementedKVStoreRaftServer
}

func newRKVRPCServer(node raft.INode, wg *sync.WaitGroup) *rkvRPCServer {
	return &rkvRPCServer{
		wg:   wg,
		node: node,
	}
}

func (s *rkvRPCServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesReply, error) {
	ae := toRaftAERequest(req)
	resp, err := s.node.AppendEntries(ctx, ae)
	if err != nil {
		return nil, err
	}
	return fromRaftAEReply(resp), nil
}

func (s *rkvRPCServer) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteReply, error) {
	rv := toRaftRVRequest(req)
	resp, err := s.node.RequestVote(ctx, rv)

	if err != nil {
		return nil, err
	}
	return fromRaftRVReply(resp), nil
}

// InstallSnapshot 获取并且在当前节点安装快照
func (s *rkvRPCServer) InstallSnapshot(stream pb.KVStoreRaft_InstallSnapshotServer) error {
	//从grpc中获取快照
	recvFunc := func() (*raft.SnapshotRequestHeader, []byte, error) {
		var pbReq *pb.SnapshotRequest
		var err error
		if pbReq, err = stream.Recv(); err != nil {
			return nil, nil, err
		}
		return toRaftSnapshotRequestHeader(pbReq), pbReq.Data, nil
	}
	reader, err := raft.NewSnapshotStreamReader(recvFunc, s.node.OnSnapshot)
	if err != nil {
		return err
	}
	req, err := raft.ReceiveSnapshot(s.node.NodeID(), reader)
	if err != nil {
		return err
	}

	//安装并使用
	var reply *raft.AppendEntriesReply
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(500)*time.Millisecond)
	defer cancel()

	if reply, err = s.node.InstallSnapshot(ctx, req); err != nil {
		return err
	}

	return stream.SendAndClose(fromRaftAEReply(reply))
}

func (s *rkvRPCServer) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetReply, error) {
	cmd := toRaftSetRequest(req)

	exeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	resp, err := s.node.Execute(exeCtx, cmd)
	if err != nil {
		return nil, err
	}
	return fromRaftSetReply(resp), nil
}

func (s *rkvRPCServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetReply, error) {
	cmd := toRaftGetRequest(req)
	exeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	resp, err := s.node.Get(exeCtx, cmd)
	if err != nil {
		return nil, err
	}
	return fromRaftGetReply(resp), nil
}
func (s *rkvRPCServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteReply, error) {
	cmd := toRaftDeleteRequest(req)
	exeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	resp, err := s.node.Execute(exeCtx, cmd)
	if err != nil {
		return nil, err
	}
	return fromRaftDeleteReply(resp), nil
}

func (s *rkvRPCServer) Start(port string) {
	var opts []grpc.ServerOption
	s.server = grpc.NewServer(opts...)
	pb.RegisterKVStoreRaftServer(s.server, s)
	s.wg.Add(1)
	go func() {
		if lis, err := net.Listen("tcp", ":"+port); err != nil {
			util.Fatalf("不能与改节点建立联系:%s,Error:%s", port, err)
		} else if err := lis.Close(); err != nil {
			util.Fatalf("关闭:%s,Error:%s", port, err)
		}
		s.wg.Done()
	}()
}

// Stop 关停 rpc server
func (s *rkvRPCServer) Stop() {
	s.server.GracefulStop()
	s.wg.Wait()
}
