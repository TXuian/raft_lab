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
	currentTerm int
	votedFor int
	log []LogEntry

	// V states
	mu sync.Mutex
	state RaftStatus
	heartbeat bool

	commitIndex int
	lastApplied int
	nextIndex []int
	matchIndex []int
}

type RaftStatus int

type LogEntry struct {
	Index int
	Term int
	Cmd interface{}
}

const (
	FOLLOWER = RaftStatus(0)
	CANDIDATE = RaftStatus(1)
	LEADER = RaftStatus(2)
)

// rpc structs
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateId int
	Term int

	LastLogIndex int
	LastLogTerm int
}

type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
	Term int
}

type AppendEntryArgs struct {
	Term int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term int
	Success bool

	// Find comflict entry
	XTerm int
	XIndex int
	XLen int
}

type InstallSnapshotArgs struct {
	Term int
	LeaderId int

	LastIncludedIndex int
	LastIncludedTerm int

	// Offset int
	// Done = ture

	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}

// Raft log op
func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getFirstIndex() int {
	return rf.log[0].Index
}

func (rf *Raft) indexToNaiveIndex(index int) int {
	offset := rf.getFirstIndex()
	return index - offset
}

func (rf *Raft) naiveIndexToIndex(n_index int) int {
	offset := rf.getFirstIndex()
	return offset + n_index
}