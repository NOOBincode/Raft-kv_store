package rkv

import (
	"errors"
	"fmt"
	"github.com/bytedance/sonic"
	"io"
	"pkg/raft"
	"pkg/util"
	"sync"
)

//实现在raft中状态机的具体功能

var errorNoKeyProvidedForGet = errors.New("没有Get的目标key")

const (
	// KVCmdSet 添加一个 键值对
	KVCmdSet = 1
	// KVCmdDel 删除一个键值对
	KVCmdDel = 2
)

type KVCmdData struct {
	Key string
	Val string
}

type rkvStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func newRkvStore() *rkvStore {
	return &rkvStore{
		data: make(map[string]string),
		mu:   sync.RWMutex{},
	}
}

func (store *rkvStore) Apply(cmd raft.StateMachineCmd) {
	if cmd.CmdType != KVCmdSet && cmd.CmdType != KVCmdDel {
		util.Panicf("不是预期的kv操作:%d", cmd.CmdType)
	}
	store.mu.Lock()
	defer store.mu.Unlock()

	data := cmd.Data.(KVCmdData)
	if cmd.CmdType == KVCmdSet {
		store.data[data.Key] = data.Val
	} else if cmd.CmdType == KVCmdDel {
		delete(store.data, data.Key)
	}
}

func (store *rkvStore) Get(param ...interface{}) (result interface{}, err error) {
	if len(param) != 1 {
		return nil, errorNoKeyProvidedForGet
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	key := param[0].(string)
	if v, ok := store.data[key]; ok {
		return v, nil
	}

	return "", fmt.Errorf("key %s 不存在", key)
}

func (store *rkvStore) Serialize(w io.Writer) error {
	store.mu.RLock()
	defer store.mu.RUnlock()

	return sonic.ConfigDefault.NewEncoder(w).Encode(store.data)
}

func (store *rkvStore) Deserialize(r io.Reader) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	return sonic.ConfigDefault.NewDecoder(r).Decode(store.data)
}
