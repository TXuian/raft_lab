package raft

import "sync"

type RaftStatus int32

const (
	FOLLOWER = RaftStatus(0)
	CANDIDATE = RaftStatus(1)
	LEADER = RaftStatus(2)
)

type RaftTermSyn struct {
	term int32
	mu sync.Mutex
}

type RaftStatusSyn struct {
	status RaftStatus
	mu sync.Mutex
}

type RaftVotedForSyn struct {
	vote_for int32
	mu sync.Mutex
}

type RaftHeartbeatSyn struct {
	heartbeat bool
	mu sync.Mutex
}

// rpc structs
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateId_ int32
	Term_ int32
}

type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted_ bool
	Term_ int32
}

type AppendEntryArgs struct {
	Term_ int32
	LeaderId_ int32
}

type AppendEntryReply struct {
	Term_ int32
	Success_ bool
}

func (rf *Raft) UpdateTerm(new_term int32) {
	rf.current_term_.mu.Lock()
	defer rf.current_term_.mu.Unlock()
	if rf.current_term_.term >= new_term {
		return
	}
	rf.current_term_.term = new_term
	rf.UpdateStatus(FOLLOWER)
}

func (rf *Raft) IncreTerm() {
	rf.current_term_.mu.Lock()
	defer rf.current_term_.mu.Unlock()
	rf.current_term_.term++
}

func (rf *Raft) ReadTerm() int32 {
	rf.current_term_.mu.Lock()
	defer rf.current_term_.mu.Unlock()
	return rf.current_term_.term
}

func (rf *Raft) UpdateStatus(new_status RaftStatus) {
	rf.status_.mu.Lock()
	defer rf.status_.mu.Unlock()
	rf.status_.status = new_status
}

func (rf *Raft) ReadStatus() RaftStatus {
	rf.status_.mu.Lock()
	defer rf.status_.mu.Unlock()
	return rf.status_.status
}

func (rf *Raft) UpdateVotedFor(new_voted_for int32) {
	rf.voted_for_.mu.Lock()
	defer rf.voted_for_.mu.Unlock()
	rf.voted_for_.vote_for = new_voted_for
}

func (rf *Raft) ReadVotedFor() int32 {
	rf.voted_for_.mu.Lock()
	defer rf.voted_for_.mu.Unlock()
	return rf.voted_for_.vote_for	
}