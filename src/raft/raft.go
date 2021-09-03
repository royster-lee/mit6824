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
	"fmt"
	"math"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

// ApplyMsg
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

const (
	FOLLOWER = "follow"
	CANDIDATE = "candidate"
	LEADER = "leader"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        		sync.Mutex          // Lock to protect shared access to this peer's state
	peers     		[]*labrpc.ClientEnd // RPC end points of all peers
	persister 		*Persister          // Object to hold this peer's persisted state
	me        		int                 // this peer's index into peers[]
	dead      		int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm 	int
	voteFor			int
	state			string
	voteCount		int
	nPeer			int
	majority 		int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

func StableHeartbeatTimeout() time.Duration {
	return 1000 * time.Millisecond
}

func RandomizedElectionTimeout() time.Duration {
	return time.Duration(1000 + makeSeed() % 500) * time.Millisecond
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			if rf.state != LEADER {
				rf.mu.Lock()
				rf.ChangeState(CANDIDATE)
				rf.currentTerm += 1
				rf.StartElection()
				rf.electionTimer.Reset(RandomizedElectionTimeout())
				rf.mu.Unlock()
			}
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == LEADER {
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft)ChangeState(state string)  {
	fmt.Printf("server %d become %s\n", rf.me, state)
	rf.state = state
	rf.voteFor = -1
	rf.voteCount = 0
}


func (rf *Raft)StartElection()  {
	fmt.Printf("server %d start election\n", rf.me)
	rf.voteFor = rf.me
	rf.voteCount++
	for i:=0; i<rf.nPeer; i++ {
		if i != rf.me{
			var args RequestVoteArgs
			var reply RequestVoteReply
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			// 发送rpc到其他节点；统计票数
			ok := rf.sendRequestVote(i, &args, &reply)
			if ok && reply.VoteGranted {
				rf.voteCount++
			}
		}
	}
	if rf.voteCount >= rf.majority {
		rf.ChangeState(LEADER)
	}

}

type AppendEntriesReply struct {
	Term 	int
	Success bool
}

type AppendEntries struct {
	Term 			int
	LeaderId 		int
	PrevLogIndex 	int
	PrevLogTerm	 	int
	Entries			[]string
	LeaderCommit 	int
}

func (rf *Raft)BroadcastHeartbeat(b bool)  {
	for i:=0; i<rf.nPeer; i++ {
		if i != rf.me{
			var args AppendEntries
			var reply AppendEntriesReply
			rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
		}
	}
}

func (rf *Raft)AppendEntries(args *AppendEntries, reply *AppendEntriesReply)  {
	fmt.Printf("server %d recive AppendEntries rpc from %d\n", rf.me, args.LeaderId)
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.state == CANDIDATE {
			rf.ChangeState(FOLLOWER)
		}
	}
}



// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	isleader = rf.state == LEADER
	term = rf.currentTerm
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
	Term int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int
	VoteGranted	bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 如果自己的term不大于当前选举者得term且当前未给其他选举者投票；则投票给当前选举者;重置自己得选举计时器
	fmt.Printf("server %d recive RequestVote rpc from %d\n", rf.me, args.CandidateId)
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	if rf.state == FOLLOWER && args.Term >= rf.currentTerm && rf.voteFor == -1 {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		rf.currentTerm++
	}
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





// Start
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


//the service or tester wants to create a Raft server. the ports
//of all the Raft servers (including this one) are in peers[]. this
//server's port is peers[me]. all the servers' peers[] arrays
//have the same order. persister is a place for this server to
//save its persistent state, and also initially holds the most
//recent saved state, if any. applyCh is a channel on which the
//tester or service expects Raft to send ApplyMsg messages.
//Make() must return quickly, so it should start goroutines
//for any long-running work.

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.currentTerm = 0
	rf.persister = persister
	rf.me = me
	rf.nPeer = len(rf.peers)
	rf.voteFor = -1
	rf.majority = int(math.Ceil(float64(rf.nPeer / 2)))
	rf.electionTimer = time.NewTimer(RandomizedElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(StableHeartbeatTimeout())
	// Your initialization code here (2A, 2B, 2C).

	go rf.ticker()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
