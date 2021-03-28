package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server [%d] term %d handling RequestVote from server %d term %d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)

	if args.Term > rf.currentTerm {
		rf.becomeFollowerL(args.Term)
	}

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		DPrintf("server [%d] term %d denied RequestVote from candidate %d in term %d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	myLastLogIndex := len(rf.log) - 1
	myLastLogTerm := rf.log[myLastLogIndex].Term
	// grant vote if server hasn't voted for anyone else and the candidate's log is at least as up-to-date as server's
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > myLastLogTerm ||
			args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.mu.Unlock() // release the lock before sending on resetTimeout channel
		rf.resetTimeout()
		rf.mu.Lock()
		DPrintf("server [%d] granted vote to candidate %d in term %d\n", rf.me, args.CandidateId, args.Term)
	} else {
		DPrintf("server [%d] denied vote to candidate %d in term %d\n", rf.me, args.CandidateId, args.Term)
		reply.VoteGranted = false
	}
}

// send RequestVote RPC to server.
// returns true if voteGranted.
func (rf *Raft) callRequestVote(server, candidateTerm int) bool {
	DPrintf("candidate [%d] term %d sending RequestVote to server %d\n", rf.me, candidateTerm, server)
	// prepare arguments
	rf.mu.Lock()
	lastLogIndex := len(rf.log) - 1
	args := RequestVoteArgs{
		Term:         candidateTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.log[lastLogIndex].Term,
	}
	rf.mu.Unlock()
	reply := &RequestVoteReply{}
	for !rf.killed() { // repeat request indefinitely until we get a reply
		currentTerm, currentState := rf.getCurrentState()
		if currentTerm != candidateTerm || currentState != candidate { // check current state
			return false
		}

		gotReply := rf.peers[server].Call("Raft.RequestVote", args, reply)

		currentTerm, currentState = rf.getCurrentState()
		if currentTerm != candidateTerm || currentState != candidate { // check current state
			return false
		}
		if !gotReply { // retry after some time
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if reply.VoteGranted {
			return true
		}
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerL(reply.Term)
		}
		rf.mu.Unlock()
		return false
	}
	return false
}

// start a new election
func (rf *Raft) startElection() {
	// election preparation
	rf.mu.Lock()
	rf.votedFor = rf.me
	candidateTerm := rf.currentTerm
	totalNum := len(rf.peers)
	rf.mu.Unlock() // after fetching the arguments we need, release the lock to avoid holding the lock while sending RPC

	DPrintf("candidate [%d] term %d starting election\n", rf.me, candidateTerm)

	var votesRcvd int32 // votes counter
	votesRcvd = 1
	finished := 1
	var voteMutex sync.Mutex         // guard access for votesRcvd and finished
	cond := sync.NewCond(&voteMutex) // condition variable for waiting for the election to end

	// send RequestVote RPC to all servers and process the result
	for pr := range rf.peers {
		if pr == rf.me {
			continue
		}
		go func(server int) {
			voteGranted := rf.callRequestVote(server, candidateTerm)
			currentTerm, currentState := rf.getCurrentState()
			if currentTerm != candidateTerm || currentState != candidate {
				return
			}
			voteMutex.Lock()
			if voteGranted {
				votesRcvd++
				DPrintf("😊candidate [%d] term %d got vote from server %d, %d votes received\n", rf.me, server, candidateTerm, votesRcvd)
			} else {
				DPrintf("😭candidate [%d] term %d didn't got vote from server %d, the reason could be either the network loss or the voter didn't recognize the candidate. %d votes received\n", rf.me, server, candidateTerm, votesRcvd)
			}
			finished++
			voteMutex.Unlock()
			cond.Broadcast()
		}(pr)
	}

	// waiting for the vote result.
	voteMutex.Lock()
	defer voteMutex.Unlock()
	for (int(votesRcvd) <= totalNum/2) && (finished != totalNum) {
		DPrintf("candidate [%d] waiting for vote in term %d complete\n", rf.me, candidateTerm)
		cond.Wait()
		currentTerm, currentState := rf.getCurrentState()
		if currentTerm != candidateTerm || currentState != candidate {
			DPrintf("candidate [%d] term %d out of date, current term %d, stop voting\n", rf.me, candidateTerm, currentTerm)
			return
		}
	}
	DPrintf("candidate [%d] vote finished for term %d, %d/%d\n", rf.me, candidateTerm, atomic.LoadInt32(&votesRcvd), totalNum)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// become leader if win
	if int(votesRcvd) > totalNum/2 && rf.currentTerm == candidateTerm && rf.state == candidate { // double check whether election is still valid
		DPrintf("congrats! candidate [%d] won the election of term %d\n", rf.me, candidateTerm)
		rf.becomeLeaderL()
	} else { // transit back to follower if lost or voting expired
		DPrintf("oops! candidate [%d] lost the election of term %d\n", rf.me, candidateTerm)
		rf.becomeFollowerL(candidateTerm)
	}
}

// reset rf's election timer.
// sending might block!
// so never call resetTimeout while holding the lock!
func (rf *Raft) resetTimeout() {
	rf.resetTimeoutCh <- struct{}{}
}

// server state transits to follower.
// assuming lock held.
func (rf *Raft) becomeFollowerL(newTerm int) {
	// become follower only when newTerm is at least as upto date as currentTerm
	if newTerm < rf.currentTerm { // a stale request
		return
	}
	rf.state = follower
	if newTerm > rf.currentTerm { // entering a new term
		rf.currentTerm = newTerm
		rf.votedFor = -1 // currentTerm increment should always be accompanied by votedFor reset
		rf.persist()
	}
	DPrintf("😈server [%d] becomes follower in term %d\n", rf.me, rf.currentTerm)
}

// server increases term, becomes candidate and initiates an election
func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = candidate
	rf.currentTerm++
	rf.votedFor = -1
	DPrintf("💪server [%d] becomes candidate in new term %d\n", rf.me, rf.currentTerm)
	rf.persist()
	go rf.startElection()
}

// server state transit from candidate to leader.
// assuming lock held.
func (rf *Raft) becomeLeaderL() {
	DPrintf("✨candidate [%d] becomes leader in term %d\n", rf.me, rf.currentTerm)
	rf.state = leader
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(rf.peers))
	go rf.broadCastPeriodically()
}

func (rf *Raft) getCurrentState() (int, State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state
}

// generate election timeout
func genTimeout() time.Duration {
	ms := 240 + rand.Intn(240)
	return time.Duration(ms) * time.Millisecond
}
