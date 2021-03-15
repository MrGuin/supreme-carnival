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
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// logEntry
type logEntry struct {
	command interface{} // command for state machine
	term    int         // term when entry was received by leader
}

// server state
type State string

const (
	follower  State = "follower"
	candidate       = "candidate"
	leader          = "leader"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state State // current state

	// Persistent states.
	currentTerm int
	votedFor    int
	log         []*logEntry // log entries

	// Volatile states on all servers.
	commitIndex int
	lastApplied int

	// Volatile states on leaders.
	nextIndex  []int
	matchIndex []int

	// channel to reset election timeout,
	// which should be sent through only by the two RPC handler.
	resetTimeout chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == leader {
		isleader = true
	}

	return term, isleader
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

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
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
	//if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.LastLogIndex >= rf.commitIndex {
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetTimeout <- struct{}{}
		DPrintf("server %d granted vote to candidate %d in term %d\n", rf.me, args.CandidateId, args.Term)
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term     int // leader's term
	LeaderId int
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term    int // currentTerm, for leader to updaate itself
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server %d handling heartbeat from server %d term %d\n", rf.me, args.LeaderId, args.Term)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.resetTimeout <- struct{}{}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

	for pr := range rf.peers {
		// send RequestVote RPC to all servers and handle the result
		if pr == rf.me {
			continue
		}
		go func(server int) {
			voteGranted := rf.callRequestVote(server, candidateTerm)
			DPrintf("candidate %d RequestVote for server %d in term %d returned\n", rf.me, server, candidateTerm)
			mu.Lock()
			if voteGranted {
				votesRcvd++
				DPrintf("😊candidate %d got vote from server %d in term %d, %d votes received\n", rf.me, server, candidateTerm, votesRcvd)
			} else {
				DPrintf("😭candidate %d didn't got vote from server %d in term %d, the reason could be either the network loss or the voter didn't recognize the candidate. %d votes received\n", rf.me, server, candidateTerm, votesRcvd)
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

// send RequestVote RPC to server.
// returns true if voteGranted.
func (rf *Raft) callRequestVote(server, candidateTerm int) bool {
	DPrintf("candidate %d term %d sending RequestVote to server %d\n", rf.me, candidateTerm, server)
	args := &RequestVoteArgs{
		Term:         candidateTerm,
		CandidateId:  rf.me,
		LastLogIndex: 1,
		LastLogTerm:  candidateTerm,
		//LastLogIndex: rf.commitIndex,
		//LastLogTerm:  rf.log[rf.commitIndex].term,
	}
	reply := &RequestVoteReply{}
	for {	// repeat request indefinitely until we get a reply
		rf.mu.Lock()
		currentTerm := rf.currentTerm
		currentState := rf.state
		rf.mu.Unlock()
		if currentTerm == candidateTerm && currentState == candidate {	// check current state
			ok := rf.peers[server].Call("Raft.RequestVote", args, reply)	// send RequestVote RPC
			if ok {		// got reply
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
			continue	// no reply, repeat sending RequestVote RPC
		} else {
			return false
		}
	}
}

// send AppendEntries RPC to server.
// return true if no new leader occurs, including reply lost.
// return false only when a new leader occurs.
func (rf *Raft) callAppendEntries(server, leaderTerm int) bool {
	DPrintf("leader %d of term %d sending heartbeats to server %d\n", rf.me, leaderTerm, server)
	args := &AppendEntriesArgs{
		Term:     leaderTerm,
		LeaderId: rf.me,
	}
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		if reply.Success {
			return true
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return false
		}
	}
	return true
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
	DPrintf("server %d becomes follower in term %d\n", rf.me, rf.currentTerm)
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
	go rf.broadCastPeriodically()
}

// for a leader to broadcast heartbeats periodically to all servers.
func (rf *Raft) broadCastPeriodically() {
	rf.mu.Lock()
	leaderTerm := rf.currentTerm
	rf.mu.Unlock()
	DPrintf("leader %d in term %d start broadCast heartbeats to all servers\n", rf.me, leaderTerm)
	var done int32 = 0 // new leader emerges
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
				ok := rf.callAppendEntries(server, leaderTerm)
				if !ok { // a new leader of higher term occurred
					atomic.StoreInt32(&done, 1)
				}
			}(pr)
		}
		if atomic.LoadInt32(&done) == 1 {
			return
		}
		time.Sleep(120 * time.Millisecond)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.state = follower
	rf.votedFor = -1
	//rf.log = make([]*logEntry)
	rf.resetTimeout = make(chan struct{})

	DPrintf("server %d term %d initialized\n", rf.me, rf.currentTerm)

	// start election timeout watcher
	go func() {
		DPrintf("server %d election timeout watcher started\n", rf.me)
		for !rf.killed() {
			electionTimeout := genTimeout()
			select {
			case <-time.After(electionTimeout):
				// become candidate and start election only when server is not current leader
				_, isLeader := rf.GetState()
				if !isLeader {
					rf.becomeCandidate()
				}
			case <-rf.resetTimeout:
				DPrintf("server %d election timeout reset\n", rf.me)
				continue
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// generate election timeout
func genTimeout() time.Duration {
	ms := 240 + rand.Intn(240)
	return time.Duration(ms) * time.Millisecond
}

func init() {
	rand.Seed(time.Now().UnixNano())
	logFile, _ := os.Create("log" + time.Now().String())
	log.SetOutput(logFile)
}
