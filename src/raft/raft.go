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

// save Raft's persistent state to stable storage,
func (rf *Raft) encodeState() []byte {
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
	return data
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// DPrintf("[Persist] data: %v", data)
	rf.persister.SaveRaftState(rf.encodeState())
}

// restore previously persisted state.
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check index
	index_offset := rf.getFirstIndex()
	if index < index_offset {
		DPrintf("[Snapshot] old snapshot\n")
		return
	}

	// truncate log
	arr := rf.log[index - index_offset:]
	ArrDeepCopy(&arr)
	// guard entry
	rf.log[0].Cmd = nil

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
}

// code that change stats ----------------------------
// toLeader: 
// 	called by sendRequestVote(in mu)
func (rf *Raft) toLeader() {
	// change state
	rf.state = LEADER
	// make nextIndex and matchIndex
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for p := range rf.nextIndex {
		rf.nextIndex[p] = rf.getLastIndex() + 1
		rf.matchIndex[p] = 0
	}
	// go rf.Start(0)
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

// RPC receiver ------------------------------------
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
		(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[len(rf.log)-1].Index) {
		is_update_log = true
	}

	if (rf.votedFor == args.CandidateId || rf.votedFor == -1) && is_update_log {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		DPrintf("[RequestVote Follower] S%d get from S%d, args: %v\n", rf.me, args.CandidateId, args)
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

	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = rf.naiveIndexToIndex(len(rf.log)) 

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
	if args.PrevLogIndex > rf.getLastIndex() {
		// not exist entry
		reply.XIndex = len(rf.log)
		reply.Success = false
		return
	} else if rf.log[rf.indexToNaiveIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		// conflict entry
		reply.Success = false
		reply.XTerm = rf.log[rf.indexToNaiveIndex(args.PrevLogIndex)].Term
		// first index of conflit term
		for n_idx := range rf.log {
			if rf.log[n_idx].Term == reply.XTerm {
				reply.XIndex = rf.log[n_idx].Index 
				break
			}
		}
		return
	}

	// append
	for offset, log_entry := range args.Entries {
		index := args.PrevLogIndex + 1 + offset
		if index >= rf.naiveIndexToIndex(len(rf.log)) {
			rf.log = append(rf.log, log_entry)
		} else if rf.log[rf.indexToNaiveIndex(index)].Term != log_entry.Term {
			rf.log = append(rf.log[: rf.indexToNaiveIndex(index)], log_entry)
		}
	}

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.naiveIndexToIndex(len(rf.log) - 1))
	}

	reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	// check term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.toFollower()
	} else if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm
	// build snapshot: data is snapshot
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	n_idx := len(rf.log) - 1 
	for ; n_idx >= 0; n_idx -- {
		if rf.log[n_idx].Index == args.LastIncludedIndex && 
			rf.log[n_idx].Term == args.LastIncludedTerm {
			arr := rf.log[n_idx:]
			ArrDeepCopy(&arr)
		}
	}
	if n_idx == -1 {
		// last entry of snapshot not exists
		rf.log = make([]LogEntry, 1)
		rf.log[0] = LogEntry{
			Index: args.LastIncludedIndex,
			Term: args.LastIncludedTerm,
			Cmd: nil,
		}
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot: args.Data,
			SnapshotTerm: args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	} ()
}

// RPC sender --------------------------------------

func (rf *Raft) sendRequestVote() {
	// build args and replys, reset election clock
	rf.mu.Lock()
	args := RequestVoteArgs {
		CandidateId: rf.me,
		Term: rf.currentTerm,
		LastLogIndex: rf.naiveIndexToIndex(len(rf.log) - 1),
		LastLogTerm: rf.log[len(rf.log)-1].Term,
	}
	reply := RequestVoteReply {}
	rf.heartbeat = true

	DPrintf("[RequestVote Candidate] S%d\n", rf.me)
	rf.mu.Unlock()

	voteGet := 1
	for p := range rf.peers {
		if p == rf.me { continue } 
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
			if rf.state != CANDIDATE || args.Term != reply_inside.Term { return }
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

// rf.updateCommitIndex(), called by sendAppendEntry(in mu)
func (rf *Raft) updateCommitIndex() {
	tmp := make([]int, len(rf.matchIndex))
	copy(tmp, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(tmp)))
	majority_index := tmp[len(tmp) / 2]

	if rf.currentTerm == rf.log[rf.indexToNaiveIndex(majority_index)].Term && majority_index > rf.commitIndex {
		rf.commitIndex = majority_index
	}
}

func (rf *Raft) handleDelayedReplicator(p int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs {
		Term: rf.currentTerm,
		LeaderId: rf.me,
		LastIncludedIndex: rf.naiveIndexToIndex(0),
		LastIncludedTerm: rf.log[0].Term,
		Data: rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply {}
	DPrintf("[InstallSnapshot send] S%d sendInstallSnapshot to S%d, LastIncludedIndex: %v, lastIndex: %d, args: %v\n", rf.me, p, rf.naiveIndexToIndex(0), rf.log[len(rf.log)-1].Index, args)
	rf.mu.Unlock()

	ok := rf.peers[p].Call("Raft.InstallSnapshot", &args, &reply)
	if !ok { return }

	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()
	DPrintf("[InstallSnapeshot reply] S%d get reply from S%d, reply: %v\n", rf.me, p, reply)

	// check term
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.toFollower()
	}
	if rf.state != LEADER || rf.currentTerm != reply.Term { return }

	rf.matchIndex[p] = args.LastIncludedIndex
	rf.nextIndex[p] = rf.matchIndex[p] + 1
}

func (rf *Raft) sendAppendEntry() {
	// send AppendEntry
	reply := AppendEntryReply {}

	for p := range rf.peers {
		if p == rf.me {
			rf.mu.Lock()
			rf.nextIndex[p] = rf.naiveIndexToIndex(len(rf.log))
			rf.matchIndex[p] = rf.nextIndex[p] - 1
			rf.heartbeat = true
			rf.mu.Unlock()
			continue
		}

		go func(p int, reply_inside AppendEntryReply) {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}

			prev_log_index := rf.nextIndex[p] - 1
			// handle delayed replicator
			if prev_log_index < rf.naiveIndexToIndex(0) {
				rf.mu.Unlock()
				rf.handleDelayedReplicator(p)
				return
			}

			// do sendAppendEntry
			args := AppendEntryArgs {
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: prev_log_index,
				PrevLogTerm: rf.log[rf.indexToNaiveIndex(prev_log_index)].Term,
				Entries: rf.log[rf.indexToNaiveIndex(prev_log_index + 1):],
				LeaderCommit: rf.commitIndex,
			}
			DPrintf("[AppendEntry send] S%d sendAppendEntry to S%d, nextIndex: %v, len(log): %d, args: %v\n", rf.me, p, rf.nextIndex[p], len(rf.log), args)

			rf.mu.Unlock()

			ok := rf.peers[p].Call("Raft.AppendEntry", &args, &reply_inside);

			// failed RPC
			if !ok { return }
			DPrintf("[AppendEntry reply] S%d get AppendEntry Reply from S%d, isLeader: %v, reply: %v\n", rf.me, p, (rf.state == LEADER), reply_inside)

			rf.mu.Lock()
			defer rf.persist()
			defer rf.mu.Unlock()
			if reply_inside.Term > rf.currentTerm {
				rf.currentTerm = reply_inside.Term
				rf.toFollower()
			} 
			if rf.state != LEADER || args.Term != reply_inside.Term { return }
			if reply_inside.Success {  // replciate succeed
				// update nextIndex and matchIndex
				rf.matchIndex[p] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[p] = rf.matchIndex[p] + 1

				// update commitIndex
				rf.updateCommitIndex()
			} else {  // replicate failed
				// avoid missed case
				// handle failed replicator
				if reply_inside.XTerm == -1 {
					// case 3
					rf.nextIndex[p] = reply_inside.XLen // XLen < len(log)
					return
				}

				x_term_index := -1
				for n_idx := args.PrevLogIndex; n_idx >= 0; n_idx-- {
					if rf.log[n_idx].Term == reply_inside.XTerm {
						x_term_index = rf.log[n_idx].Index
						break
					}
				}
				if x_term_index == -1 {
					// case 1: leader doesn't has XTerm
					rf.nextIndex[p] = reply_inside.XIndex
				} else {
					// case 2: leader has XTerm
					rf.nextIndex[p] = x_term_index + 1 
				}
			}
		} (p, reply)
	}
}

// Raft Client Interface ---------------------------

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
			Index: rf.log[len(rf.log)-1].Index + 1,
			Term: rf.currentTerm,
			Cmd: command,
		}
		rf.log = append(rf.log, log_entry)
	} 
	return rf.log[len(rf.log)-1].Index, rf.currentTerm, rf.state == LEADER
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

// Raft long-run gorotine -----------------------------

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
const (
	HEARTBEAT_TIMEOUT = time.Millisecond * 100
	MIN_ELECTION_TIMEOUT = 250
	MAX_ELECTION_TIMEOUT = 400
	COMMIT_TIME_GAP = time.Millisecond * 10
)

func (rf *Raft) isNeedApply() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastApplied <= rf.commitIndex
}

func (rf *Raft) commitEntries() {
	for !rf.killed() {
		// sleep
		time.Sleep(COMMIT_TIME_GAP)
		// commit entries 
		for rf.isNeedApply() {
			rf.mu.Lock()
			apply_msg := ApplyMsg{
				CommandValid: true,
				Command: rf.log[rf.indexToNaiveIndex(rf.lastApplied)].Cmd,
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
		time_to_sleep := time.Millisecond * time.Duration(MIN_ELECTION_TIMEOUT + rand.Intn(MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT))
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
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	rf.log = append(rf.log, LogEntry{
		Index: 0,
		Cmd: 0,
		Term: 0,
	})
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.lastApplied = rf.log[0].Index

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.commitEntries()


	return rf
}
