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
	"bytes"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

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

	stream := new(bytes.Buffer)
	encoder := labgob.NewEncoder(stream)
	encoder.Encode(rf.current_term_.ReadMemberSync())
	encoder.Encode(rf.voted_for_.ReadMemberSync())
	encoder.Encode(rf.logs)
	data := stream.Bytes()
	rf.persister.SaveRaftState(data)
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

	stream := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(stream)

	var current_term int32
	var voted_for int32
	var logs []LogEntry

	rf.log_mu_.Lock()
	defer rf.log_mu_.Unlock()
	if decoder.Decode(current_term) != nil ||
		decoder.Decode(voted_for) != nil ||
		decoder.Decode(logs) != nil {
		DPrintf("[Persister] error read persist state.\n")
	} else {
		rf.current_term_.UpdateMemberSync(current_term)
		rf.voted_for_.UpdateMemberSync(voted_for)
		rf.logs = logs
	}
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
		return
	} 

	// B: vote for yes
	// update rule
	rf.log_mu_.Lock()
	update := false
	if args.LastLogTerm_ > rf.logs[len(rf.logs)-1].Term_ {
		update = true
	}
	if args.LastLogTerm_ == rf.logs[len(rf.logs)-1].Term_ &&
		args.LastLogIndex_ >= int32(len(rf.logs) - 1) {
		update = true
	}
	rf.log_mu_.Unlock()	

	// update server info
	if args.Term_ > rf.current_term_.ReadMemberSync() {
		rf.current_term_.UpdateMemberSync(args.Term_)
		rf.status_.UpdateMemberSync(FOLLOWER)
		rf.voted_for_.UpdateMemberSync(-1)
		rf.persist()
	}

	// do the vote
	reply.Term_ = rf.current_term_.ReadMemberSync()
	voted_for := rf.voted_for_.ReadMemberSync()
	if (voted_for == args.CandidateId_ || voted_for == -1) && update {
		rf.voted_for_.UpdateMemberSync(args.CandidateId_)
		rf.persist()
		reply.VoteGranted_ = true
		// granting vote means a heartbeat
		rf.heartbeat_.UpdateMemberSync(true)
	} else {
		reply.VoteGranted_ = false
	}
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.heartbeat_.UpdateMemberSync(true)
	
	// update term if needed
	if args.Term_ > rf.current_term_.ReadMemberSync() {
		rf.current_term_.UpdateMemberSync(args.Term_)
		rf.status_.UpdateMemberSync(FOLLOWER)
		rf.persist()
	}

	// deal with entries received
	// 1. check current_term
	// 2. check log consistency
	reply.Term_ = rf.current_term_.ReadMemberSync()

	rf.log_mu_.Lock()
	defer rf.log_mu_.Unlock()
	if (args.Term_ < rf.current_term_.ReadMemberSync()) ||
		((len(rf.logs) - 1) < int(args.PrevLogIndex_)) || 
		(rf.logs[args.PrevLogIndex_].Term_ != args.PrevLogTerm_ ) {
		reply.Success_ = false
		return
	}
	// update log: len(log) - 1 >= PrevLogIndex
	if (int(args.PrevLogIndex_) >= len(rf.logs)) ||
		(rf.logs[args.PrevLogIndex_].Term_ != args.PrevLogTerm_) {
		if int(args.PrevLogIndex_) < len(rf.logs) {
			rf.logs = rf.logs[0:args.PrevLogIndex_]
			rf.persist()
		}	
		return
	}
	rf.logs = append(rf.logs[0: args.PrevLogIndex_ + 1], args.Entries...)
	rf.persist()

	if rf.commit_index.ReadMemberSync() < args.LeaderCommit_ {
		rf.commit_index.UpdateMemberSync(
			Min(args.LeaderCommit_, int32(len(rf.logs) - 1)))
	}
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
	rf.persist()
	rf.status_.UpdateMemberSync(CANDIDATE)

	// prepare for rpc
	rf.log_mu_.Lock()
	args := RequestVoteArgs {
		Term_: rf.current_term_.ReadMemberSync(),
		CandidateId_: int32(rf.me),

		LastLogIndex_: int32(len(rf.logs) - 1),
		LastLogTerm_: rf.logs[len(rf.logs)-1].Term_,
	}
	rf.log_mu_.Unlock()
	reply := RequestVoteReply {
		VoteGranted_: false,
	}
	
	vote_get := 1
	vote_chan := make(chan bool)
	// request vote from peers 
	rf.voted_for_.UpdateMemberSync(int32(rf.me))
	rf.persist()
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
			// me become leader!
			rf.status_.UpdateMemberSync(LEADER)

			// update next_index and match_index_
			rf.log_mu_.Lock()
			rf.next_index_ = make([]RaftMemberSync[int32], len(rf.peers))
			for idx := range rf.next_index_ {
				rf.next_index_[idx].UpdateMemberSync(int32(len(rf.logs)))
			}
			rf.match_index_ = make([]RaftMemberSync[int32], len(rf.peers))
			for idx := range rf.match_index_ {
				rf.match_index_[idx].UpdateMemberSync(0)
			}
			rf.log_mu_.Unlock()

			go rf.sendHeartBeat()
			return 
		}
		if rf.status_.ReadMemberSync() != CANDIDATE { 
			break 
		} 
	}
}

func getMajoritySameIndex(matchIndex []RaftMemberSync[int32]) int {     
    tmp := make([]int, len(matchIndex))
	for idx := range tmp {
		tmp[idx] = int(matchIndex[idx].ReadMemberSync())
	}  
    sort.Sort(sort.Reverse(sort.IntSlice(tmp)))

    idx := len(tmp) / 2
    return tmp[idx]
}

func (rf *Raft) sendAppendEntry() {
	// send AppendEntry
	reply := AppendEntryReply {
		Success_: false,
	}

	for p := range rf.peers {
		if p == rf.me {
			rf.heartbeat_.UpdateMemberSync(true)
			rf.log_mu_.Lock()
			rf.next_index_[p].UpdateMemberSync(int32(len(rf.logs)))
			rf.match_index_[p].UpdateMemberSync(int32(len(rf.logs) - 1))
			rf.log_mu_.Unlock()
			continue  
		}

		prevLogIndex := int(rf.next_index_[p].ReadMemberSync() - 1)
		rf.log_mu_.Lock()
		args := AppendEntryArgs {
			Term_: rf.current_term_.ReadMemberSync(),
			LeaderId_: int32(rf.me),

			PrevLogIndex_: int32(prevLogIndex),
			PrevLogTerm_: rf.logs[prevLogIndex].Term_,

			Entries: rf.logs[prevLogIndex + 1:],
			LeaderCommit_: rf.commit_index.ReadMemberSync(),
		}
		rf.log_mu_.Unlock()

		go func(p int, reply_inside AppendEntryReply) {
			ok := rf.peers[p].Call("Raft.AppendEntry", &args, &reply_inside)
			
			rf.log_mu_.Lock()
			if ok && reply_inside.Success_ {
				rf.match_index_[p].UpdateMemberSync(args.PrevLogIndex_ + int32(len(args.Entries)))
				rf.next_index_[p].UpdateMemberSync(rf.match_index_[p].ReadMemberSync() + 1)

				majority_index_ := getMajoritySameIndex(rf.match_index_)
				// if (rf.logs[majority_index_].Term_ == rf.current_term_.ReadMemberSync()) && 
				// 	(majority_index_ > int(rf.commit_index.ReadMemberSync())) {
				if majority_index_ > int(rf.commit_index.ReadMemberSync()) {
					rf.commit_index.UpdateMemberSync(int32(majority_index_))
				}
			} else{ 
				if reply_inside.Term_ > rf.current_term_.ReadMemberSync() {
					rf.current_term_.UpdateMemberSync(reply.Term_)
					rf.persist()
					rf.status_.UpdateMemberSync(FOLLOWER)
				}
				old_next_index := rf.next_index_[p].ReadMemberSync() - 1
				if old_next_index < 1 {
					old_next_index = 1
				}
				rf.next_index_[p].UpdateMemberSync(old_next_index) 
			}
			rf.log_mu_.Unlock()
		} (p, reply)
	}
	
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
		rf.logs = append(rf.logs, log_entry)
		rf.persist()
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
	HEARTBEAT_TIMEOUT = time.Millisecond * 70
	MIN_ELECTION_TIMEOUT = time.Millisecond * 150
	MAX_ELECTION_TIMEOUT = time.Millisecond * 300
	ELECTION_TIME_GAP = MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT
)

func (rf *Raft) commitEntries() {
	old_commit_index := 1
	for !rf.killed() {
		// sleep
		time.Sleep(HEARTBEAT_TIMEOUT)
		// commit entries 
		for int32(old_commit_index) <= rf.commit_index.ReadMemberSync() {
			rf.log_mu_.Lock()
			apply_msg := ApplyMsg{
				CommandValid: true,
				Command: rf.logs[old_commit_index].Cmd_,
				CommandIndex: int(old_commit_index),
			}
			rf.log_mu_.Unlock()
			rf.applyCh <- apply_msg
			old_commit_index++
		}
	}
}

func (rf *Raft) sendHeartBeat() {
	for rf.status_.ReadMemberSync() == LEADER {
		go rf.sendAppendEntry()
		time.Sleep(HEARTBEAT_TIMEOUT)
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
	rf.persist()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.commitEntries()


	return rf
}
