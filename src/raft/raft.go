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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	stream := new(bytes.Buffer)
	encoder := labgob.NewEncoder(stream)

	// !persist contains lock
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// encode data
	err := encoder.Encode(rf.currentTerm)
	if err != nil {
		DPrintf("[Persist] Encode term err: %v\n", err)
	}
	err = encoder.Encode(rf.votedFor)
	if err != nil {
		DPrintf("[Persist] voted_for err: %v\n", err)
	}
	err = encoder.Encode(rf.log)
	if err != nil {
		DPrintf("[Persist] logs err: %v\n", err)
	}

	// save data
	data := stream.Bytes()
	// DPrintf("[Persist] data: %v", data)
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2C).
	stream := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(stream)

	var current_term int
	var voted_for int
	var logs []LogEntry

	if decoder.Decode(&current_term) != nil ||
		decoder.Decode(&voted_for) != nil ||
		decoder.Decode(&logs) != nil {
		DPrintf("[Err] readRersist err.\n")
	} else {
		rf.currentTerm = current_term
		rf.votedFor = voted_for
		rf.log = logs
		// DPrintf("[ReadPersist] %d, term: %d, voteFor: %v, log: %v", rf.me, current_term, voted_for, logs)
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

// code that change stats
// toLeader: 
// 	called by sendRequestVote(in mu)
func (rf *Raft) toLeader() {
	// change state
	rf.state = LEADER
	// make nextIndex and matchIndex
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = len(rf.log)
		rf.matchIndex[idx] = 0
	}
	go rf.Start(0)
	go rf.sendHeartBeat()
}

// toCandidate:
// 	called by: ticker(in mu)
func (rf *Raft) toCandidate() {
	// change state
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	go rf.sendRequestVote()
}

// toFollower:
// 	called by: everytime received RPC(in mu)
func (rf *Raft) toFollower() {
	rf.state = FOLLOWER
	// new follower
	rf.votedFor = -1
	rf.heartbeat = true
}

// RPC receiver
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	// check term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.toFollower()
	} else if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// check log state
	is_update_log := false
	if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
		(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log) - 1) {
		is_update_log = true
	}

	if (rf.votedFor == args.CandidateId || rf.votedFor == -1) && is_update_log {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// reset only when vote!
		rf.heartbeat = true
	} else {
		reply.VoteGranted = false
	}
	
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()
	DPrintf("S%d get from S%d, currentTerm: %d, args: %v, len(log): %d\n", rf.me, args.LeaderId, rf.currentTerm, args, len(rf.log));

	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = len(rf.log)

	// check term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.toFollower()
	} else if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return 
	}

	rf.heartbeat = true

	reply.Term = rf.currentTerm
	// check log correct
	if args.PrevLogIndex >= len(rf.log) {
		// not exist entry
		// reply.XTerm = rf.log[len(rf.log)-1].Term
		// reply.XIndex = len(rf.log) - 1
		reply.XIndex = len(rf.log)
		reply.Success = false
		return
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// conflict entry
		reply.Success = false
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		for idx := range rf.log {
			if rf.log[idx].Term == reply.XTerm {
				reply.XIndex = idx 
				break
			}
		}
		return
	}

	// append
	for offset, log_entry := range args.Entries {
		index := args.PrevLogIndex + 1 + offset
		if index >= len(rf.log) {
			rf.log = append(rf.log, log_entry)
		} else if rf.log[index].Term != log_entry.Term {
			rf.log = append(rf.log[: index], log_entry)
		}
	}

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log) - 1)
	}

	reply.Success = true
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

	rf.mu.Lock()
	args := RequestVoteArgs {
		CandidateId: rf.me,
		Term: rf.currentTerm,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm: rf.log[len(rf.log)-1].Term,
	}
	reply := RequestVoteReply {}
	rf.heartbeat = true
	rf.mu.Unlock()

	voteGet := 0
	for p := range rf.peers {
		go func(p int, reply_inside RequestVoteReply) {
			ok := rf.peers[p].Call("Raft.RequestVote", &args, &reply_inside);
			if !ok { return }
			rf.mu.Lock()
			defer rf.persist()
			defer rf.mu.Unlock()
			if reply_inside.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.toFollower()
			}
			if rf.state != CANDIDATE { return }
			if reply_inside.VoteGranted {
				// get voted!
				voteGet++
				if voteGet > (len(rf.peers) / 2) {
					// become leader
					rf.toLeader()
				}
			} 
		} (p, reply)
	}

}

func getMajoritySameIndex(matchIndex []int) int {     
	tmp := make([]int, len(matchIndex))
	copy(tmp, matchIndex)
    sort.Sort(sort.Reverse(sort.IntSlice(tmp)))
    return tmp[len(tmp) / 2]
}

func (rf *Raft) sendAppendEntry() {
	// send AppendEntry
	reply := AppendEntryReply {}

	for p := range rf.peers {
		if p == rf.me {
			rf.nextIndex[p] = len(rf.log)
			rf.matchIndex[p] = rf.nextIndex[p] - 1
			continue
		}

		go func(p int, reply_inside AppendEntryReply) {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}

			prev_log_index := rf.nextIndex[p] - 1
			DPrintf("S%d sendAppendEntry to S%d, nextIndex: %v, len(log): %d\n", rf.me, p, rf.nextIndex[p], len(rf.log))
			args := AppendEntryArgs {
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: prev_log_index,
				PrevLogTerm: rf.log[prev_log_index].Term,
				Entries: rf.log[prev_log_index + 1:],
				LeaderCommit: rf.commitIndex,
			}

			rf.mu.Unlock()

			ok := rf.peers[p].Call("Raft.AppendEntry", &args, &reply_inside);
			if !ok { return }

			rf.mu.Lock()
			defer rf.persist()
			defer rf.mu.Unlock()
			if reply_inside.Term > rf.currentTerm {
				rf.currentTerm = reply_inside.Term
				rf.toFollower()
			} 
			if rf.state != LEADER || rf.currentTerm != reply_inside.Term { return }
			if reply_inside.Success {
				// replciate succeed
				// update nextIndex and matchIndex
				rf.matchIndex[p] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[p] = rf.matchIndex[p] + 1

				// update commitIndex
				majority_index := getMajoritySameIndex(rf.matchIndex)
				if rf.log[majority_index].Term == rf.currentTerm && majority_index > rf.commitIndex {
					rf.commitIndex = majority_index	
				}
			} else {
				// replicate failed
				// avoid missed case
				if reply_inside.XTerm == -1 {
					// case 3
					rf.nextIndex[p] = reply_inside.XLen // XLen < len(log)
					DPrintf("S%d get AppendEntry Reply from S%d, isLeader: %v, reply: %v\n", rf.me, p, (rf.state == LEADER), reply_inside)
					return
				}

				x_term_index := -1
				for idx := args.PrevLogIndex; idx >= 0; idx-- {
					if rf.log[idx].Term == reply_inside.XTerm {
						x_term_index = idx
						break
					}
				}
				if x_term_index == -1 {
					// case 1: leader doesn't has XTerm
					rf.nextIndex[p] = reply_inside.XIndex
				} else {
					// case 2: leader has XTerm
					rf.nextIndex[p] = Min(x_term_index + 1, reply_inside.XLen) 
				}
				DPrintf("S%d get AppendEntry Reply from S%d, isLeader: %v, reply: %v\n", rf.me, p, (rf.state == LEADER), reply_inside)
				if rf.nextIndex[p] == 0 {
					DPrintf("S%d, x_term_index: %d, XLen: %d, server log: %v\n", rf.me, x_term_index, reply_inside.XLen, rf.log)
				}
			}
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

	// Your code here (2B).
	// Leader path
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()
	if rf.state == LEADER {
		log_entry := LogEntry{
			Term: rf.currentTerm,
			Cmd: command,
		}
		rf.log = append(rf.log, log_entry)
	} 
	return len(rf.log) - 1, rf.currentTerm, rf.state == LEADER
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
	MIN_ELECTION_TIMEOUT = time.Millisecond * 250
	MAX_ELECTION_TIMEOUT = time.Millisecond * 400
	ELECTION_TIME_GAP = MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT
	COMMIT_TIME_GAP = time.Millisecond * 10
)

func (rf *Raft) commitEntries() {
	for !rf.killed() {
		// sleep
		time.Sleep(COMMIT_TIME_GAP)
		// commit entries 
		for rf.lastApplied <= rf.commitIndex {
			rf.mu.Lock()
			apply_msg := ApplyMsg{
				CommandValid: true,
				Command: rf.log[rf.lastApplied].Cmd,
				CommandIndex: int(rf.lastApplied),
			}
			rf.mu.Unlock()
			rf.applyCh <- apply_msg
			rf.lastApplied++
		}
	}
}

func (rf *Raft) sendHeartBeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.mu.Unlock()	
			rf.sendAppendEntry()
			time.Sleep(HEARTBEAT_TIMEOUT)
		} else {
			rf.mu.Unlock()
			return	
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// get timeout to sleep
		time_to_sleep := MIN_ELECTION_TIMEOUT + (time.Duration(rand.Int()) % ELECTION_TIME_GAP)
		time.Sleep(time_to_sleep)
		// check heartbeat
		rf.mu.Lock()
		if !rf.heartbeat {
			rf.toCandidate()
		}
		rf.heartbeat = false
		rf.mu.Unlock()
		rf.persist()
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
	rf := &Raft{
		peers: peers,
		persister: persister,
		me: me,
		applyCh: applyCh,

		currentTerm: 0,
		votedFor: -1,
		log: make([]LogEntry, 0),

		state: FOLLOWER,
		heartbeat: false,
		
		commitIndex: 0,
		lastApplied: 0,
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	rf.log = append(rf.log, LogEntry{
		Cmd: 0,
		Term: 0,
	})
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.commitEntries()


	return rf
}
