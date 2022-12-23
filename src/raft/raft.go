package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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
	current_term_ RaftMemberSync[int32]
	voted_for_ RaftMemberSync[int32]
	logs []LogEntry
	log_mu_ sync.Mutex

	// V states
	status_ RaftMemberSync[RaftStatus]
	heartbeat_ RaftMemberSync[bool]

	commit_index RaftMemberSync[int32]
	last_applied RaftMemberSync[int32]
	next_index_ []RaftMemberSync[int32]
	match_index_ []RaftMemberSync[int32]

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return int(rf.current_term_.ReadMemberSync()), rf.status_.ReadMemberSync() == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// rpc receiver
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// A: vote for not
	if args.Term_ < rf.current_term_.ReadMemberSync() {
		reply.Term_ = rf.current_term_.ReadMemberSync()
		reply.VoteGranted_ = false
		//DPrintf("[Server] %d vote to %d in term %d\n", rf.me, rf.voted_for_.ReadMemberSync(), rf.current_term_.ReadMemberSync())
		return
	} 

	// B: vote for yes
	// update server info
	if args.Term_ > rf.current_term_.ReadMemberSync() {
		rf.current_term_.UpdateMemberSync(args.Term_)
		rf.status_.UpdateMemberSync(FOLLOWER)
		rf.voted_for_.UpdateMemberSync(-1)
	}

	// do the vote
	reply.Term_ = rf.current_term_.ReadMemberSync()
	voted_for := rf.voted_for_.ReadMemberSync()
	if voted_for == args.CandidateId_ || voted_for == -1 {
		rf.voted_for_.UpdateMemberSync(args.CandidateId_)
		reply.VoteGranted_ = true
		// granting vote means a heartbeat
		rf.heartbeat_.UpdateMemberSync(true)
	} else {
		reply.VoteGranted_ = false
	}
	//DPrintf("[Server] %d vote to %d in term %d\n", rf.me, rf.voted_for_.ReadMemberSync(), rf.current_term_.ReadMemberSync())
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// DPrintf("[Follower] %d receive appendentry, %v\n", rf.me, args)
	//DPrintf("[Follower] %d logs: %v\n", rf.me, rf.logs)
	rf.heartbeat_.UpdateMemberSync(true)
	// update term if needed
	if args.Term_ > rf.current_term_.ReadMemberSync() {
		rf.current_term_.UpdateMemberSync(args.Term_)
		rf.status_.UpdateMemberSync(FOLLOWER)
	}

	// deal with entries received
	// 1. check current_term
	// 2. check log consistency
	reply.FollowerId_ = int32(rf.me)
	reply.Term_ = rf.current_term_.ReadMemberSync()

	rf.log_mu_.Lock()
	defer rf.log_mu_.Unlock()
	if (args.Term_ < rf.current_term_.ReadMemberSync()) ||
		((len(rf.logs) - 1) < int(args.PrevLogIndex_)) || 
		(rf.logs[len(rf.logs)-1].Term_ != args.PrevLogTerm_ ) {
		reply.Success_ = false
		return
	}
	// update log: len(log) - 1 >= PrevLogIndex
	for index, entry := range args.Entries {
		index_in_log := args.PrevLogIndex_ + 1 + int32(index)
		// DPrintf("[Follower] %d, log: %v, index_in_log: %d\n", rf.me, rf.logs, index_in_log)
		if (len(rf.logs) > int(index_in_log)) && 
			(rf.logs[index_in_log] != entry) {
			// truncate slice
			// DPrintf("[Follower] %d receive appendentry, %v\n", rf.me, args)
			// DPrintf("[Follower] %d logs: %v\n", rf.me, rf.logs)

			rf.logs = rf.logs[0:index_in_log]
		} 
		if len(rf.logs) <= int(index_in_log) {
			rf.logs = append(rf.logs, entry)
		}
	}
	if rf.commit_index.ReadMemberSync() < args.LeaderCommit_ {
		rf.commit_index.UpdateMemberSync(
			Min(args.LeaderCommit_, int32(len(rf.logs) - 1)))
	}
	// DPrintf("[Server] %d, args: %v, log: %v, commit idx: %d", rf.me, args, rf.logs, rf.commit_index.ReadMemberSync())
	reply.Success_ = true
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote() {
	// vote one is from me self
	// incre term_
	rf.current_term_.UpdateMemberSync(rf.current_term_.ReadMemberSync() + 1)
	rf.status_.UpdateMemberSync(CANDIDATE)

	// prepare for rpc
	args := RequestVoteArgs {
		Term_: rf.current_term_.ReadMemberSync(),
		CandidateId_: int32(rf.me),
	}
	reply := RequestVoteReply {
		VoteGranted_: false,
	}
	//DPrintf("sendRequestVote sender: %d, term %d\n", rf.me, rf.current_term_.ReadMemberSync())
	
	vote_get := 1
	vote_chan := make(chan bool)
	// request vote from peers 
	rf.voted_for_.UpdateMemberSync(int32(rf.me))
	for p := range rf.peers {
		if p == rf.me { continue }
		go func(p int, reply_inside RequestVoteReply) {
			ok := rf.peers[p].Call("Raft.RequestVote", &args, &reply_inside)
			if ok && reply_inside.VoteGranted_ {
				vote_chan <- true
			} 
		} (p, reply)
	}

	for p := range rf.peers {
		if p == rf.me { continue }
		// gather vote
		if vote_res := <- vote_chan; vote_res {
			vote_get++
		}

		// check if valid to exit vote step
		if vote_get > (len(rf.peers) / 2) {
			rf.status_.UpdateMemberSync(LEADER)
			go rf.sendHeartBeat()
		}
		if rf.status_.ReadMemberSync() != CANDIDATE { 
			break 
		} 
	}
}

func (rf *Raft) sendAppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	//DPrintf("[Leader] sendAppendEntry, sender %d, term %d, entries: %v\n", rf.me, rf.current_term_.ReadMemberSync(), args.Entries)
	// send AppendEntry
	append_entry_ch := make(chan AppendEntryReply)
	for p := range rf.peers {
		go func(p int, reply_inside AppendEntryReply) {
			ok := rf.peers[p].Call("Raft.AppendEntry", args, &reply_inside)
			if ok {
				append_entry_ch <- reply_inside 
			}
		} (p, *reply)
	}
	
	// TODO: deal with results
	success_cnt := 1
	for range rf.peers {
		cur_reply := <- append_entry_ch
		if cur_reply.FollowerId_ == int32(rf.me) {
			continue
		}
		// update term if needed
		rf.dealAppendResult(args, &cur_reply, &success_cnt)
	}
}

func (rf *Raft) sendSingleAppendEntry(peer int32, args *AppendEntryArgs, reply *AppendEntryReply) {
	ok := false
	for !ok {
		ok := rf.peers[peer].Call("Raft.AppendEntry", args, reply)
		if ok {
			if reply.Term_ > rf.current_term_.ReadMemberSync() {
				rf.current_term_.UpdateMemberSync(reply.Term_)
				rf.status_.UpdateMemberSync(FOLLOWER)
			}
			if !reply.Success_ {
				rf.dealAppendFail(args, *reply)
			}
		}
	}
}

func (rf *Raft) dealAppendResult(args *AppendEntryArgs, reply *AppendEntryReply, success_cnt *int) {
	if reply.Term_ > rf.current_term_.ReadMemberSync() {
		rf.current_term_.UpdateMemberSync(reply.Term_)
		rf.status_.UpdateMemberSync(FOLLOWER)
	}
	if !reply.Success_ {
		go rf.dealAppendFail(args, *reply)
	} else {
		*success_cnt++
	}
	if (*success_cnt > len(rf.peers) / 2) && (len(args.Entries) != 0) {
		// //DPrintf("[Leader] Apply commit, %v\n", args.Entries)
		if rf.commit_index.ReadMemberSync() <= args.PrevLogIndex_ {
			rf.commit_index.UpdateMemberSync(Min(int32(len(rf.logs)) - 1, args.PrevLogIndex_ + int32(len(args.Entries))))
		}
		*success_cnt = 0
	}
}

func (rf *Raft) dealAppendFail(args *AppendEntryArgs, reply AppendEntryReply) {
	// communicate to follower
	//DPrintf("[Leader] dealAppendFail to %d\n", reply.FollowerId_)
	if args.PrevLogIndex_ == 0 {
		return
	}
	// update args
	rf.log_mu_.Lock()
	new_entries := []LogEntry {
		rf.logs[args.PrevLogIndex_],
	}
	new_entries = append(new_entries, args.Entries...)
	new_args := AppendEntryArgs {
		Term_: args.Term_,
		LeaderId_: args.LeaderId_,

		PrevLogIndex_: args.PrevLogIndex_ - 1,
		PrevLogTerm_: rf.logs[args.PrevLogIndex_ - 1].Term_,

		Entries: new_entries,
		LeaderCommit_: rf.commit_index.ReadMemberSync(),
	}
	rf.log_mu_.Unlock()
	rf.sendSingleAppendEntry(reply.FollowerId_, &new_args, &reply)
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	isLeader := rf.status_.ReadMemberSync() == LEADER

	// Your code here (2B).
	// Leader path
	rf.log_mu_.Lock()
	defer rf.log_mu_.Unlock()
	if isLeader {
		log_entry := LogEntry{
			Term_: rf.current_term_.ReadMemberSync(),
			Cmd_: command,
		}
		// append log
		args := AppendEntryArgs{
			Term_: rf.current_term_.ReadMemberSync(),
			LeaderId_: int32(rf.me),

			PrevLogIndex_: int32(len(rf.logs)) - 1,
			PrevLogTerm_: rf.logs[len(rf.logs)-1].Term_,
			
			Entries: []LogEntry{log_entry},
			LeaderCommit_: rf.commit_index.ReadMemberSync(),
		}
		reply := AppendEntryReply {
			Success_: false,
		}
		rf.logs = append(rf.logs, log_entry)

		// append entry to peers
		go rf.sendAppendEntry(&args, &reply)
	} 
	// follower(no-leader) path
	return len(rf.logs) - 1, int(rf.current_term_.ReadMemberSync()), isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
const (
	HEARTBEAT_TIMEOUT = time.Millisecond * 100
	MIN_ELECTION_TIMEOUT = time.Millisecond * 300
	MAX_ELECTION_TIMEOUT = time.Millisecond * 500
	ELECTION_TIME_GAP = MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT
)

func (rf *Raft) commitEntries() {
	old_commit_index := 1
	for !rf.killed() {
		// sleep
		time.Sleep(HEARTBEAT_TIMEOUT)
		// commit entries 
		for int32(old_commit_index) <= rf.commit_index.ReadMemberSync() {
			//DPrintf("[Server] %d commit %v\n", rf.me, rf.logs[old_commit_index])
			rf.log_mu_.Lock()
			apply_msg := ApplyMsg{
				CommandValid: true,
				Command: rf.logs[old_commit_index].Cmd_,
				CommandIndex: int(old_commit_index),
			}
			rf.log_mu_.Unlock()
			rf.applyCh <- apply_msg
			DPrintf("[Server] %d Commited %v, %d", rf.me, rf.logs[old_commit_index], old_commit_index)
			old_commit_index++
		}
	}
}

func (rf *Raft) sendHeartBeat() {
	for rf.status_.ReadMemberSync() == LEADER {
		time.Sleep(HEARTBEAT_TIMEOUT)
		rf.log_mu_.Lock()
		args := AppendEntryArgs{
			Term_: rf.current_term_.ReadMemberSync(),
			LeaderId_: int32(rf.me),

			PrevLogIndex_: int32(len(rf.logs)) - 1,
			PrevLogTerm_: rf.logs[len(rf.logs)-1].Term_,
			
			LeaderCommit_: rf.commit_index.ReadMemberSync(),
		}
		rf.log_mu_.Unlock()
		reply := AppendEntryReply{}
		go rf.sendAppendEntry(&args, &reply)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// get timeout to sleep
		time_to_sleep := MIN_ELECTION_TIMEOUT + (time.Duration(rand.Int()) % ELECTION_TIME_GAP)
		rf.heartbeat_.UpdateMemberSync(false)
		time.Sleep(time_to_sleep)
		// check heartbeat
		if !rf.heartbeat_.ReadMemberSync() {
			go rf.sendRequestVote()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.log_mu_.Lock()
	rf.logs = append(rf.logs, LogEntry{
		Cmd_: 0,
		Term_: 0,
	})
	rf.log_mu_.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.commitEntries()


	return rf
}
