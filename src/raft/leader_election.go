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
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server %d handling RequestVote from server %d term %d\n", rf.me, args.CandidateId, args.Term)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("server %d in term %d denied RequestVote from candidate %d in term %d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	myLastLogIndex := len(rf.log) - 1
	myLastLogTerm := rf.log[myLastLogIndex].Term
	// grant vote if server hasn't voted for anyone else and the candidate's log is at least as up-to-date as server's
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > myLastLogTerm ||
			args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetTimeout()
		DPrintf("server %d granted vote to candidate %d in term %d\n", rf.me, args.CandidateId, args.Term)
	} else {
		DPrintf("server %d denied vote to candidate %d in term %d\n", rf.me, args.CandidateId, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

// send RequestVote RPC to server.
// returns true if voteGranted.
func (rf *Raft) callRequestVote(server, candidateTerm int) bool {
	DPrintf("candidate %d term %d sending RequestVote to server %d\n", rf.me, candidateTerm, server)
	// prepare arguments
	rf.mu.Lock()
	lastLogIndex := len(rf.log) - 1
	args := RequestVoteArgs{
		Term:         candidateTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.log[lastLogIndex].Term,
		//LastLogIndex: rf.commitIndex,
		//LastLogTerm:  rf.log[rf.commitIndex].term,
	}
	rf.mu.Unlock()
	reply := &RequestVoteReply{}
	for !rf.killed() { // repeat request indefinitely until we get a reply
		rf.mu.Lock()
		currentTerm := rf.currentTerm
		currentState := rf.state
		rf.mu.Unlock()
		if currentTerm == candidateTerm && currentState == candidate { // check current state
			ok := rf.peers[server].Call("Raft.RequestVote", args, reply) // send RequestVote RPC
			if ok {                                                      // got reply
				if reply.VoteGranted {
					return true
				}
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
				}
				rf.mu.Unlock()
				return false
			}
			time.Sleep(50 * time.Millisecond)
			continue // no reply, repeat sending RequestVote RPC
		} else {
			return false
		}
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

	DPrintf("candidate %d term %d starting election\n", rf.me, candidateTerm)

	var votesRcvd int32 // votes counter
	votesRcvd = 1
	finished := 1
	var mu sync.Mutex         // guard access for votesRcvd and finished
	cond := sync.NewCond(&mu) // condition variable for waiting for the election to end

	// send RequestVote RPC to all servers and process the result
	for pr := range rf.peers {
		if pr == rf.me {
			continue
		}
		go func(server int) {
			voteGranted := rf.callRequestVote(server, candidateTerm)
			DPrintf("candidate %d RequestVote for server %d in term %d returned\n", rf.me, server, candidateTerm)
			mu.Lock()
			if voteGranted {
				votesRcvd++
				//DPrintf("😊candidate %d got vote from server %d in term %d, %d votes received\n", rf.me, server, candidateTerm, votesRcvd)
			} else {
				//DPrintf("😭candidate %d didn't got vote from server %d in term %d, the reason could be either the network loss or the voter didn't recognize the candidate. %d votes received\n", rf.me, server, candidateTerm, votesRcvd)
			}
			finished++
			mu.Unlock()
			cond.Broadcast()
		}(pr)
	}

	// waiting for the vote result.
	mu.Lock()
	for (int(votesRcvd) <= totalNum/2) && (finished != totalNum) {
		DPrintf("candidate %d waiting for vote in term %d complete\n", rf.me, candidateTerm)
		cond.Wait()
	}
	DPrintf("candidate %d vote finished for term %d, %d/%d\n", rf.me, candidateTerm, atomic.LoadInt32(&votesRcvd), totalNum)
	defer mu.Unlock()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// become leader if win
	if int(votesRcvd) > totalNum/2 && rf.currentTerm == candidateTerm && rf.state == candidate { // double check whether election is still valid
		DPrintf("congrats! candidate %d got votes from majority in the election of term %d, and will become leader soon\n", rf.me, candidateTerm)
		rf.becomeLeader()
	} else { // transit back to follower if lost or voting expired
		DPrintf("oops! candidate %d didn't get enough votes in the election of term %d, election lost\n", rf.me, candidateTerm)
		if candidateTerm == rf.currentTerm { // check currentTerm
			rf.becomeFollower(candidateTerm)
		}
	}
}

// reset rf's election timer.
func (rf *Raft) resetTimeout() {
	rf.resetTimeoutCh <- struct{}{}
}

// server state transits to follower.
// assuming lock held.
func (rf *Raft) becomeFollower(newTerm int) {
	// become follower only when newTerm is at least as upto date as currentTerm
	if newTerm < rf.currentTerm { // a stale request
		return
	}
	rf.state = follower
	if newTerm > rf.currentTerm { // entering a new term
		rf.currentTerm = newTerm
		rf.votedFor = -1 // currentTerm increment should always be accompanied by votedFor reset
	}
	DPrintf("😈server %d becomes follower in term %d\n", rf.me, rf.currentTerm)
}

// server increases term, becomes candidate and initiates an election
func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	rf.state = candidate
	rf.currentTerm++
	rf.votedFor = -1
	DPrintf("💪server %d becomes candidate in new term %d\n", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	go rf.startElection()
}

// server state transit from candidate to leader.
// assuming lock held.
func (rf *Raft) becomeLeader() {
	DPrintf("✨candidate %d becomes leader in term %d\n", rf.me, rf.currentTerm)
	rf.state = leader
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(rf.peers))
	go rf.broadCastPeriodically()
}

// generate election timeout
func genTimeout() time.Duration {
	ms := 240 + rand.Intn(240)
	return time.Duration(ms) * time.Millisecond
}
