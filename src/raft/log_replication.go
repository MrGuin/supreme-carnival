package raft

import (
	"sync"
	"sync/atomic"
	"time"
)

// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // for follower to redirect
	PrevLogIndex int        // new log entry insert position
	PrevLogTerm  int        // term of insert position
	Entries      []LogEntry // log entries to store
	LeaderCommit int        // leader's commitIndex
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to updaate itself
	Success bool // consistency check passed
}

// AppendEntries RPC.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server %d handling AppendEntries from server %d of term %d\n", rf.me, args.LeaderId, args.Term)
	//DPrintf("server %d term %d before handling AppendEntries, logs: %v\n", rf.me, rf.currentTerm, rf.log[1:])
	reply.Term = rf.currentTerm     // set term for leader to update itself
	if args.Term < rf.currentTerm { // request from outdated leader
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm { // request from a new leader, update currentTerm
		rf.becomeFollower(args.Term)
	}
	rf.resetTimeout() // reset election timeout

	if args.PrevLogIndex >= len(rf.log) ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // consistency check
		//fmt.Printf("server %d consistency check failed\n", rf.me)
		reply.Success = false
		return
	}
	if len(args.Entries) > 0 { // new entries to append
		// find first conflict entry index
		conflictIndex := args.PrevLogIndex + 1
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
		}
		DPrintf("server %d in term %d append new entries: [%d, %d]\n", rf.me, rf.currentTerm, conflictIndex, len(rf.log)-1)
	}
	if args.LeaderCommit > rf.commitIndex { // AppendEntries RPC no.5
		newCommitIndex := args.LeaderCommit
		if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Index < args.LeaderCommit {
			newCommitIndex = args.Entries[len(args.Entries)-1].Index
		}
		rf.commitIndex = newCommitIndex
		DPrintf("server %d in term %d commitIndex updated: %d\n", rf.me, rf.currentTerm, rf.commitIndex)

		// apply logs
		rf.notifyApply()
		//DPrintf("server %d term %d lastApplied: %d, commitIndex: %d\n", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
	}
	reply.Success = true
	DPrintf("server %d term %d after handling AppendEntries, logs: %v\n", rf.me, rf.currentTerm, rf.log[1:])
}

// send AppendEntries RPC to server.
// return true if no new leader occurs, including reply lost.
// return false only when a new leader occurs.
func (rf *Raft) callAppendEntries(server, leaderTerm int, cond *sync.Cond) bool {
	DPrintf("leader %d of term %d keep sending AppendEntries RPC to server %d until getting a reply\n", rf.me, leaderTerm, server)
	defer DPrintf("leader %d of term %d callAppendEntries to server %d returns\n", rf.me, leaderTerm, server)
	for !rf.killed() {
		rf.mu.Lock()
		if rf.currentTerm != leaderTerm { // term changed, return
			rf.mu.Unlock()
			return false
		}

		DPrintf("leader %d term %d send AppendEntries RPC to server %d, log: %v\n", rf.me, leaderTerm, server, rf.log[1:])
		// prepare arguments
		lastLogIndex := len(rf.log) - 1     // lastLogIndex of leader
		nextIndex := rf.nextIndex[server]   // nextIndex of follower
		prevLogIndex := nextIndex - 1       // prevLogIndex immediately preceding new log entries
		matchIndex := rf.matchIndex[server] // matchIndex of follower, for future double check
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
		DPrintf("leader %d term %d send AppendEntries RPC to server %d, args: %+v\n", rf.me, leaderTerm, server, args)

		rf.mu.Unlock()
		reply := &AppendEntriesReply{}

		// send AppendEntries RPC
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

		// process result
		rf.mu.Lock()
		//defer rf.mu.Unlock()
		if rf.currentTerm != leaderTerm { // outdated
			rf.mu.Unlock()
			return false
		}
		if !ok { // no reply, repeat sending RPC
			DPrintf("leader %d of term %d didn't get AppendEntries reply from server %d, retry later\n", rf.me, leaderTerm, server)
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			continue
		}

		if reply.Term > leaderTerm { // new leader occurred
			rf.mu.Unlock()
			return false
		}
		if reply.Success {
			DPrintf("leader %d of term %d got success reply from server %d, update nextIndex and matchIndex if needed", rf.me, leaderTerm, server)
			if rf.nextIndex[server] == nextIndex { // double check, make sure nextIndex[server] hasn't changed
				rf.nextIndex[server] = lastLogIndex + 1
				DPrintf("leader %d of term %d update nextIndex[%d]: %d\n", rf.me, leaderTerm, server, rf.nextIndex[server])
			}
			if rf.matchIndex[server] == matchIndex { // double check
				rf.matchIndex[server] = lastLogIndex
				DPrintf("leader %d of term %d update matchIndex[%d]: %d\n", rf.me, leaderTerm, server, rf.matchIndex[server])
			}
			cond.Broadcast()
			rf.mu.Unlock()
			return true
		} else { // consistency check failed, decrement nextIndex and retry immediately
			DPrintf("leader %d of term %d: server %d consistency check failed, decrement nextIndex[server] and retry immediately\n", rf.me, leaderTerm, server)
			rf.nextIndex[server]--
			rf.mu.Unlock()
		}
	}
	return false
}

// for a leader to broadcast heartbeats or AppendEntries RPC periodically to all servers.
func (rf *Raft) broadCastPeriodically() {
	rf.mu.Lock()
	leaderTerm := rf.currentTerm
	N := rf.commitIndex + 1 // for commit check
	rf.mu.Unlock()
	DPrintf("leader %d in term %d start broadCast heartbeats to all servers\n", rf.me, leaderTerm)
	var done int32 = 0 // new leader emerges

	// commitIndex update watcher
	cond := sync.NewCond(&rf.mu) // condition variable
	go func() {
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
			for !rf.majorityCheck(N) {
				cond.Wait()
			}
			rf.commitIndex = N
			DPrintf("leader %d of term %d commitIndex updated: %d\n", rf.me, leaderTerm, N)

			DPrintf("leader %d of term %d lastApplied: %d, commitIndex :%d\n", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)

			rf.notifyApply()
			//if rf.commitIndex > rf.lastApplied {
			//	go rf.sendApplyMsg(rf.log[rf.lastApplied+1:rf.commitIndex+1], leaderTerm)
			//	rf.lastApplied = rf.commitIndex
			//}
			N++
		}
	}()

	for !rf.killed() {
		// before sending heartbeats, check if rf.me is still a valid leader
		curTerm, isLeader := rf.GetState()
		if !isLeader || curTerm != leaderTerm { // rf.me is no longer leader or/and leaderTerm out of date
			return
		}
		for pr := range rf.peers {
			if pr == rf.me {
				continue
			}
			go func(server int) {
				ok := rf.callAppendEntries(server, leaderTerm, cond)
				if !ok { // a new leader of higher term occurred
					atomic.StoreInt32(&done, 1)
				}
			}(pr)
		}
		if atomic.LoadInt32(&done) == 1 {
			DPrintf("leader %d of term %d is no longer leader, stop broadcasting\n", rf.me, leaderTerm)
			return
		}
		time.Sleep(120 * time.Millisecond)
	}
}

// check if log entries before(including) index N is replicated on majority of the cluster.
func (rf *Raft) majorityCheck(N int) bool {
	if N >= len(rf.log) || rf.log[N].Term != rf.currentTerm { // entry of last term, return
		return false
	}
	count := 0
	for pr, _ := range rf.peers {
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
