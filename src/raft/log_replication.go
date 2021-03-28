package raft

import (
	"sync"
	"time"
)

// AppendEntriesArgs
// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // for follower to redirect
	PrevLogIndex int        // new log entry insert position
	PrevLogTerm  int        // term of insert position
	Entries      []LogEntry // log entries to store
	LeaderCommit int        // leader's commitIndex
}

// AppendEntriesReply
// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term                      int  // currentTerm, for leader to updaate itself
	Success                   bool // consistency check passed
	ConflictingTerm           int  // term of conflicting entry
	ConflictingTermFirstIndex int  // first index server stores for ConflictingTerm
}

// AppendEntries RPC.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server [%d] current term %d start handling AppendEntries from leader %d of term %d\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	//DPrintf("server %d term [%d] before handling AppendEntries, logs: %v\n", rf.me, rf.currentTerm, rf.log[1:])
	reply.Term = rf.currentTerm     // set term for leader to update itself
	if args.Term < rf.currentTerm { // request from outdated leader
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm { // request from a new leader, update currentTerm
		rf.becomeFollowerL(args.Term)
	}
	rf.mu.Unlock()
	rf.resetTimeout() // reset election timeout
	rf.mu.Lock()

	if args.PrevLogIndex >= len(rf.log) ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // consistency check
		conflictTerm := args.PrevLogTerm
		firstIndex := len(rf.log) // first index of conflicting term entries
		if args.PrevLogIndex < len(rf.log) {
			firstIndex = args.PrevLogIndex
		}
		for firstIndex > 1 && rf.log[firstIndex-1].Term == conflictTerm {
			firstIndex--
		}

		reply.Success = false
		reply.ConflictingTerm = conflictTerm
		reply.ConflictingTermFirstIndex = firstIndex

		//fmt.Printf("server %d consistency check failed\n", rf.me)
		return
	}
	if len(args.Entries) > 0 { // new entries to append
		conflictIndex := args.PrevLogIndex + 1 // find first conflicting entry index
		entryIndex := 0
		for conflictIndex < len(rf.log) && entryIndex < len(args.Entries) {
			if rf.log[conflictIndex].Term != args.Entries[entryIndex].Term {
				break
			}
			conflictIndex++
			entryIndex++
		}
		if entryIndex < len(args.Entries) { // still entries left in args
			rf.log = rf.log[:conflictIndex]
			rf.log = append(rf.log, args.Entries[entryIndex:]...)
			rf.persist()
			DPrintf("server [%d] in term %d append new entries: [%d, %d]\n", rf.me, rf.currentTerm, conflictIndex, len(rf.log)-1)
		}
	}
	if args.LeaderCommit > rf.commitIndex { // AppendEntries RPC no.5
		newCommitIndex := args.LeaderCommit
		if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Index < args.LeaderCommit {
			newCommitIndex = args.Entries[len(args.Entries)-1].Index
		}
		rf.commitIndex = newCommitIndex
		DPrintf("server [%d] in term %d commitIndex updated: %d\n", rf.me, rf.currentTerm, rf.commitIndex)
		rf.notifyApply() // notify apply logs
		//DPrintf("server [%d] term %d lastApplied: %d, commitIndex: %d\n", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
	}
	reply.Success = true
	//DPrintf("server [%d] current term %d finish handling AppendEntries from leader %d of term %d, logs: %v\n", rf.me, rf.currentTerm, args.LeaderId, args.Term, rf.log[1:])
}

// send AppendEntries RPC to server and handle the reply.
// return true if no new leader occurs, including reply lost.
// return false only when a new leader occurs.
func (rf *Raft) callAppendEntries(server, leaderTerm int, commitCond *sync.Cond) {
	//DPrintf("leader [%d] of term %d keep sending AppendEntries RPC to server %d until getting a reply\n", rf.me, leaderTerm, server)
	//defer DPrintf("leader [%d] of term %d callAppendEntries to server %d returns\n", rf.me, leaderTerm, server)
	//retryCount := 0
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	for !rf.killed() {
		// DPrintf("callAppendEntries retry count: %d\n", retryCount)
		currentTerm, _ := rf.getCurrentState()
		if currentTerm != leaderTerm {
			return
		}

		// DPrintf("leader [%d] term %d send AppendEntries RPC to server %d, log: %+v\n", rf.me, leaderTerm, server, rf.log[1:])
		// prepare arguments
		rf.mu.Lock()
		lastLogIndex := len(rf.log) - 1     // lastLogIndex of leader
		nextIndex := rf.nextIndex[server]   // nextIndex of follower, for future double check
		matchIndex := rf.matchIndex[server] // matchIndex of follower, for future double check
		prevLogIndex := nextIndex - 1       // prevLogIndex immediately preceding new log entries
		args := AppendEntriesArgs{
			Term:         leaderTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  rf.log[prevLogIndex].Term,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		if lastLogIndex >= nextIndex { // leader has new log entries for follower
			entries := rf.log[nextIndex : lastLogIndex+1]
			args.Entries = make([]LogEntry, len(entries))
			copy(args.Entries, entries) // important! make a copy, instead of using log directly!
			//args.Entries = rf.log[nextIndex : lastLogIndex+1]
		}
		rf.mu.Unlock()
		reply := &AppendEntriesReply{}

		// send AppendEntries RPC
		gotReply := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		DPrintf("leader [%d] term %d send AppendEntries RPC to server %d, args: %+v\n", rf.me, leaderTerm, server, args)

		// process result
		currentTerm, _ = rf.getCurrentState()
		if currentTerm != leaderTerm {
			return
		}
		if !gotReply { // no reply, repeat sending RPC
			//DPrintf("leader [%d] of term %d didn't get AppendEntries reply from server %d, retry later\n", rf.me, leaderTerm, server)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		rf.mu.Lock()
		if reply.Term > leaderTerm { // new leader occurred
			rf.becomeFollowerL(reply.Term)
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			DPrintf("leader [%d] of term %d got success reply from server %d, update nextIndex and matchIndex if needed", rf.me, leaderTerm, server)
			if rf.nextIndex[server] == nextIndex && lastLogIndex >= nextIndex { // double check, in case nextIndex[server] has changed
				rf.nextIndex[server] = lastLogIndex + 1
				DPrintf("leader [%d] of term %d update nextIndex[%d]: %d\n", rf.me, leaderTerm, server, rf.nextIndex[server])
			}
			if rf.matchIndex[server] == matchIndex && lastLogIndex > matchIndex { // double check
				rf.matchIndex[server] = lastLogIndex
				DPrintf("leader [%d] of term %d update matchIndex[%d]: %d\n", rf.me, leaderTerm, server, rf.matchIndex[server])
			}
			if commitCond != nil {
				commitCond.Broadcast() // wake up committer goroutine
			}
			rf.mu.Unlock()
			return
		} else { // consistency check failed, decrement nextIndex and retry immediately
			DPrintf("leader %d of term %d: server %d consistency check failed, decrement nextIndex[server] and retry immediately\n", rf.me, leaderTerm, server)
			nextIndex := reply.ConflictingTermFirstIndex
			for nextIndex > 1 && rf.log[nextIndex-1].Term == reply.ConflictingTerm { // skip conflicting term
				nextIndex--
			}
			rf.nextIndex[server] = nextIndex
			// rf.nextIndex[server]--
			rf.mu.Unlock()
		}
		// DPrintf("AppendEntries try %d\n", i)
		//retryCount++
	}
}

// for a leader to broadcast heartbeats or AppendEntries RPC periodically to all servers.
func (rf *Raft) broadCastPeriodically() {
	rf.mu.Lock()
	leaderTerm := rf.currentTerm
	N := rf.commitIndex + 1 // for commit check
	rf.mu.Unlock()
	DPrintf("leader [%d] in term %d start broadcasting AE to all servers\n", rf.me, leaderTerm)
	defer DPrintf("leader [%d] in term %d stop broadcasting AE to all servers\n", rf.me, leaderTerm)

	// commitIndex update watcher
	commitCond := sync.NewCond(&rf.mu) // condition variable
	go rf.logCommitter(N, leaderTerm, commitCond)

	for !rf.killed() {
		// before sending heartbeats, check if rf.me is still a valid leader
		curTerm, isLeader := rf.GetState()
		if !isLeader || curTerm != leaderTerm { // rf.me is no longer leader or/and leaderTerm out of date
			//DPrintf("leader [%d] term %d is no longer leader, stop broadcasting\n", rf.me, curTerm)
			return
		}
		rf.broadcastAppendEntries(leaderTerm, commitCond)
		time.Sleep(120 * time.Millisecond)
	}
}

func (rf *Raft) logCommitter(N int, leaderTerm int, cond *sync.Cond) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.currentTerm != leaderTerm {
			return
		}
		if N < len(rf.log) && rf.log[N].Term != leaderTerm {
			// important! only wait when log[N].Term is current term!
			N++
			continue
		}
		for !rf.majorityCheckL(N) {
			cond.Wait()
		}
		rf.commitIndex = N
		DPrintf("leader [%d] of term %d commitIndex updated: %d\n", rf.me, leaderTerm, N)

		DPrintf("leader [%d] of term %d lastApplied: %d, commitIndex :%d\n", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)

		rf.notifyApply()
		N++
	}
}

// broadcast AppendEntries RPC to all followers.
func (rf *Raft) broadcastAppendEntries(leaderTerm int, cond *sync.Cond)  {
	//leaderTerm := rf.currentTerm
	for pr := range rf.peers {
		if pr == rf.me {
			continue
		}
		go rf.callAppendEntries(pr, leaderTerm, cond)
	}
}

// check if log entries before(including) index N is replicated on majority of the cluster.
// assuming lock held.
func (rf *Raft) majorityCheckL(N int) bool {
	if N >= len(rf.log) || rf.log[N].Term != rf.currentTerm { // entry of last term, return
		return false
	}
	count := 0
	for pr := range rf.peers {
		if rf.matchIndex[pr] >= N {
			count++
		}
	}
	return count > len(rf.peers)/2
}

// notify applier to apply committed entries.
func (rf *Raft) notifyApply() {
	rf.applyCond.Broadcast()
}
