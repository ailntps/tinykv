package message

import (
	"time"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

type Callback struct {
	Resp *raft_cmdpb.RaftCmdResponse
	Txn  *badger.Txn // used for GetSnap
	done chan struct{}
}

//Done if Callback isn't nil,put Respone in Resp and put empty struct in
//channel.
func (cb *Callback) Done(resp *raft_cmdpb.RaftCmdResponse) {
	if cb == nil {
		return
	}
	if resp != nil {
		cb.Resp = resp
	}
	cb.done <- struct{}{}
}

//WaitResp when receive a reply from channel,return respone
func (cb *Callback) WaitResp() *raft_cmdpb.RaftCmdResponse {
	select {
	case <-cb.done:
		return cb.Resp
	}
}

//WaitRespWithTimeout like WaitResp,becase it's timeout like don't use
func (cb *Callback) WaitRespWithTimeout(timeout time.Duration) *raft_cmdpb.RaftCmdResponse {
	select {
	case <-cb.done:
		return cb.Resp
	case <-time.After(timeout):
		return cb.Resp
	}
}

//NewCallback return a new Callback
func NewCallback() *Callback {
	done := make(chan struct{}, 1)
	cb := &Callback{done: done}
	return cb
}
