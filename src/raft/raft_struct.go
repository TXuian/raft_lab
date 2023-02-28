package raft

import (
	"sync"

	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// NV states
	current_term_ RaftMemberSync[int]
	voted_for_ RaftMemberSync[int]
	logs []LogEntry

	// V states
	log_mu_ sync.Mutex
	status_ RaftMemberSync[RaftStatus]
	heartbeat_ RaftMemberSync[bool]

	commit_index RaftMemberSync[int]
	last_applied RaftMemberSync[int]
	next_index_ []RaftMemberSync[int]
	match_index_ []RaftMemberSync[int]

}

type RaftStatus int

type LogEntry struct {
	Term_ int
	Cmd_ interface{}
}

const (
	FOLLOWER = RaftStatus(0)
	CANDIDATE = RaftStatus(1)
	LEADER = RaftStatus(2)
)

type MemberType interface {
	int | bool | RaftStatus 
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
	CandidateId_ int
	Term_ int

	LastLogIndex_ int
	LastLogTerm_ int
}

type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted_ bool
	Term_ int
}

type AppendEntryArgs struct {
	Term_ int
	LeaderId_ int

	PrevLogIndex_ int
	PrevLogTerm_ int
	Entries []LogEntry
	LeaderCommit_ int
}

type AppendEntryReply struct {
	Term_ int
	Success_ bool

	// Find comflict entry
	XTerm_ int
	XIndex_ int
	XLen_ int
}
