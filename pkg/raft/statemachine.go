package raft

import "io"

type StateMachineCmd struct {
	CmdType int
	Data    interface{}
}

type IValueGetter interface {
	Get(param ...interface{}) (interface{}, error)
}

type IStateMachine interface {
	Apply(cmd StateMachineCmd)
	Serialize(io.Writer) error
	Deserialize(reader io.Reader) error
	IValueGetter
}
