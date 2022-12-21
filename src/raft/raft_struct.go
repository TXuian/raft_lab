package raft

import "sync"

type RaftStatus int32

type LogEntry struct {
	Term_ int32
	Cmd_ interface{}
}

const (
	FOLLOWER = RaftStatus(0)
	CANDIDATE = RaftStatus(1)
	LEADER = RaftStatus(2)
)

type MemberType interface {
	int32 | bool | RaftStatus 
}

type RaftMemberSync[T MemberType] struct {
	member T
	mu sync.Mutex
}

func (m *RaftMemberSync[T]) UpdateMemberSync(new_val T) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.member = new_val
}

func (m *RaftMemberSync[T]) ReadMemberSync() T {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.member
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

	PrevLogIndex_ int32
	PrevLogTerm_ int32
	Entries []LogEntry
	LeaderCommit_ int32
}

type AppendEntryReply struct {
	FollowerId_ int32
	Term_ int32
	Success_ bool
}
