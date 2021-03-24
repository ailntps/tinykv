// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower  StateType = iota //=0
	StateCandidate                  //1
	StateLeader                     //2
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configura tion will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	randomElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	prs := make(map[uint64]*Progress)
	for _, p := range c.peers {
		prs[p] = &Progress{}
	}

	return &Raft{
		id:                    c.ID,
		heartbeatTimeout:      c.HeartbeatTick,
		electionTimeout:       c.ElectionTick,
		randomElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
		RaftLog:               newLog(c.Storage),
		Prs:                   prs,
		Lead:                  None,
		votes:                 make(map[uint64]bool),
		msgs:                  make([]pb.Message, 0),
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm := r.RaftLog.LastTerm()

	var ent []*pb.Entry
	n := uint64(len(r.RaftLog.entries))
	for i := prevLogIndex; i < n; i++ {
		ent = append(ent, &r.RaftLog.entries[i])
	}
	m := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: ent,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, m)
	// Your Code Here (2A).
}
func (r *Raft) sendRequestVote(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		LogTerm: r.RaftLog.LastTerm(),
	}
	r.msgs = append(r.msgs, msg)

}
func (r *Raft) tickForFollowerAndCandidate() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		r.Step(
			pb.Message{
				MsgType: pb.MessageType_MsgHup,
				From:    r.id,
				To:      r.id,
				Term:    r.Term,
			})

	}
}
func (r *Raft) tickForLeader() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		//r.sendHeartbeat(k)
		r.Step(
			pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				From:    r.id,
				To:      r.id,
				Term:    r.Term,
			})
	}
}
func (r *Raft) resetTime() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)

}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower, StateCandidate:
		r.tickForFollowerAndCandidate()
	case StateLeader:
		r.tickForLeader()
	}
	// Your Code Here (2A).
}

// becomeFollower transform this peer's state to Follower

func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = lead
	// Your Code Here (2A).
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.Term++
	if len(r.Prs) <= 1 {
		r.becomeLeader()
	}
	// Your Code Here (2A).
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	r.Lead = r.id
	for k, _ := range r.Prs {
		r.Prs[k].Next = r.RaftLog.LastIndex() + 1
		r.Prs[k].Match = 0
	}
	entTemp := pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex()}
	var ent []*pb.Entry
	ent = append(ent, &entTemp)

	mes := pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    r.id,
		To:      r.id,
		Entries: ent,
	}
	r.Step(mes)
	//  Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term

}

func (r *Raft) stepForFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.beginElection(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	}
}
func (r *Raft) stepForCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.beginElection(m)
	case pb.MessageType_MsgAppend:
		//candiddate rollback
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleResponeVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)

	}
}
func (r *Raft) stepForLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.handleMsgBeat()
	case pb.MessageType_MsgPropose:
		r.handleMsgPropose(m)
	case pb.MessageType_MsgHeartbeatResponse:
		//if m.term > r.term become follwer ,have do in allserverdo
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	}
}
func (r *Raft) allServerDo(m pb.Message) {
	//1.if commitIndex >lastApplied:increment lastApplied,apply log[lastApplied] to state machine
	//2.if termT>currentTerm set currentTerm =T,convert to follower
	if r.Term < m.Term {
		r.resetTime()
		r.becomeFollower(m.Term, m.From)
	}

}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	r.allServerDo(m)
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.stepForFollower(m)
	case StateCandidate:
		r.stepForCandidate(m)
	case StateLeader:
		r.stepForLeader(m)
	}
	return nil
}

//handleMsgUp
func (r *Raft) beginElection(m pb.Message) {
	r.resetTime()
	r.becomeCandidate()
	for k, _ := range r.Prs {
		if k != r.id {
			r.sendRequestVote(k)
		}
	}

}
func (r *Raft) handleMsgBeat() {
	for k, _ := range r.Prs {
		if k != r.id {
			r.sendHeartbeat(k)
		}
	}
}
func (r *Raft) handleMsgPropose(m pb.Message) {
	for i := 0; i < len(m.Entries); i++ {
		r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])

	}
	for k, _ := range r.Prs {
		if k != r.id {
			r.sendAppend(k)
		}
	}

}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	//cadidate rollback
	if r.Term == m.Term {
		r.resetTime()
		r.becomeFollower(m.Term, m.From)
		//do entry append r.Term>m.Term return false
	}

	// Your Code Here (2A).
}
func (r *Raft) handleResponeVote(m pb.Message) {
	r.votes[m.From] = !m.Reject
	agree, reject := 0, 0
	for _, v := range r.votes {
		if v {
			agree++
		} else {
			reject++
		}
	}
	if len(r.Prs) == 1 || agree > len(r.Prs)/2 {
		r.resetTime()
		r.becomeLeader()
		for p := range r.Prs {
			if p != r.id {
				r.sendHeartbeat(p)
			}
		}
	}
	if reject > len(r.Prs)/2 {
		r.resetTime()
		//None mean this follower come from no leader
		r.becomeFollower(r.Term, None)
	}
}
func (r *Raft) handleRequestVote(m pb.Message) {
	//1.reply false if term<currentTerm
	//2.if voteFor is null or candidaatedID,and candidated's Log is at least as up
	//to date as receiver's log grant vote(bigger term or same term but bigger index)
	//m.reject = false is vote and == true is reject
	reject := true
	upToDate := func(term, index uint64) bool {
		if r.RaftLog.LastTerm() < term {
			//m is more upTodate
			return true
		}
		if r.RaftLog.LastTerm() == term && r.RaftLog.LastIndex() <= index {
			return true
		}
		return false
	}
	if (r.Vote == None && r.Lead == None) || r.Vote == m.From {
		if upToDate(m.LogTerm, m.Index) {
			reject = false
			r.Vote = m.From
		}
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	reject := true
	if r.Term == m.Term {
		r.resetTime()
		reject = false
	}
	//r.Term>m.Term
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Reject:  reject,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
