package raft

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const snapshotChunkSize = 1024 * 8

var snapshotPath string
var errorInvalidSnapshotInfo = errors.New("无效的快照索引/任期")
var errorEmptySnapshot = errors.New("获取到了空快照")
var errorSnapshotFromStaleLeader = errors.New("从旧领袖中获取的快照")
var errorDifferentHeader = errors.New("从相同的快照中获取了不同的快照头")

type SnapshotStreamReader struct {
	header  *SnapshotRequestHeader
	recv    recvFunc
	partcb  partCallback
	buf     []byte
	readPtr int
}

type SnapshotStreamWriter struct {
	header *SnapshotRequestHeader
	send   sendFunc
}

// 创建一个新的SnapshotStreamReader
func newSnapshotStreamReader(recv recvFunc, partcb partCallback) (*SnapshotStreamReader, error) {
	//为了获取快照任期和快照索引所做的第一次读
	header, data, err := recv()
	if err != nil {
		return nil, err
	}
	if err == io.EOF {
		return nil, errorInvalidSnapshotInfo
	}
	if !partcb(header) {
		return nil, errorSnapshotFromStaleLeader
	}

	return &SnapshotStreamReader{header: header, recv: recv, partcb: partcb, buf: data}, nil
}

func (writer *SnapshotStreamWriter) Write(data []byte) (n int, err error) {
	n = len(data)
	err = writer.send(writer.header, data)
	return n, err
}
func (reader *SnapshotStreamReader) Read(p []byte) (n int, err error) {
	if reader.readPtr == len(reader.buf) {
		//缓冲层没数据了,去其他地方读
		header, data, err := reader.recv()
		if err != nil {
			return 0, err
		}
		if *header != *reader.header {
			return 0, errorDifferentHeader
		}
		if !reader.partcb(header) {
			return 0, errorSnapshotFromStaleLeader
		}

		reader.buf = data
		reader.readPtr = 0
	}
	n = copy(p, reader.buf[reader.readPtr:])
	reader.readPtr += n

	return n, nil
}

type recvFunc func() (*SnapshotRequestHeader, []byte, error)
type sendFunc func(*SnapshotRequestHeader, []byte) error
type partCallback func(part *SnapshotRequestHeader) bool

// SetSnapshotPath 设置快照存储路径
func SetSnapshotPath(path string) {
	snapshotPath = path
}

// openSnapshot 读取快照并且写进writer中
func openSnapshot(file string) (reader io.ReadCloser, err error) { return os.Open(file) }

// createSnapshot 通过提供的信息创建快照文件
// 后缀将会被加载快照文件后,可以通过gRPC"远程"获取并且本地创建
func createSnapshot(nodeID int, term int, index int, suffix string) (file string, closer io.WriteCloser, err error) {
	if term < 0 || index < 0 || suffix == "" {
		return "", nil, errorInvalidSnapshotInfo
	}

	fileName := fmt.Sprintf("Node%d_T%dL_%d_%s.raft-kv_snapshot", nodeID, term, index, suffix)
	fullpath := filepath.Join(snapshotPath, fileName)
	f, err := os.Create(fullpath)
	return fullpath, f, err
}

// 删除一个快照文件
func deleteSnapshot(file string) error {
	if file != "" {
		return os.Remove(file)
	}
	return nil
}

// RequestHeader returns the snapshot request header
func (reader *SnapshotStreamReader) RequestHeader() *SnapshotRequestHeader {
	return reader.header
}

func ReceiveSnapshot(nodeID int, reader *SnapshotStreamReader) (req *SnapshotRequest, err error) {
	req = &SnapshotRequest{
		SnapshotRequestHeader: *reader.RequestHeader(),
	}
	snapshotTerm := req.SnapshotTerm
	snapshotIndex := req.SnapshotIndex

	var file string
	var w io.WriteCloser
	if file, w, err = createSnapshot(nodeID, snapshotTerm, snapshotIndex, "remote"); err != nil {
		return
	}
	defer w.Close()

	//复制文件
	if _, err = io.Copy(w, reader); err != nil {
		return
	}
	req.File = file
	return
}

func SendSnapshot(file string, writer *SnapshotStreamWriter) error {
	reader, err := openSnapshot(file)
	if err != nil {
		return err
	}
	defer reader.Close()

	_, err = io.Copy(writer, reader)
	return err
}

// NewSnapshotStreamWriter creates a new gRPCSnapshotStreamWriter
func NewSnapshotStreamWriter(header *SnapshotRequestHeader, send sendFunc) *SnapshotStreamWriter {
	return &SnapshotStreamWriter{
		header: header,
		send:   send,
	}
}
