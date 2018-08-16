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

import "sync"
import (
	"labrpc"
	"go/ast"
)

// import "bytes"
// import "labgob"



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

const (
	Follower = iota
	Candidate
	Leader
)

type LogEntry struct {
	Command interface{}
	Term int
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state on all servers
	currentTerm int
	votedFor int
	logs []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leader
	nextIndex []int
	matchIndex []int

	// Others
	status int
	voteCount int
	applyCh chan ApplyMsg
	electWin chan bool
	granted chan bool
	heartbeat chan bool


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
	isleader = rf.status == Leader
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
	term int //candidate’s term
	candidateId int //candidate requesting vote
	lastLogIndex int //index of candidate’s last log entry (§5.4)
	lastLogTerm int //term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	term int //currentTerm, for candidate to update itself
	voteGranted bool //true means candidate received vote
}

type AppendEntriesArg struct {
	term int
	leaderId int
	prevLogIndex int
	prevLogTerm int
	entries []LogEntry
	leaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	nextTryIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.currentTerm > args.term {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.nextTryIndex =rf.getLastIndex() +1
		return
	}
	if rf.getLastIndex() < args.prevLogIndex  {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.nextTryIndex =rf.getLastIndex() + 1
		return
	}
	if rf.logs[args.prevLogIndex].Term != args.prevLogTerm {
		term := rf.logs[args.prevLogIndex].Term
		for reply.nextTryIndex = args.prevLogIndex -1; reply.nextTryIndex > 0 && rf.logs[reply.nextTryIndex].Term == term;
		 reply.nextTryIndex--{

		}
		reply.nextTryIndex++
	}

}
func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getLastTerm() int {
	return rf.logs[len(rf.logs) - 1].Term
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.currentTerm > args.term {
		reply.term = rf.currentTerm
		reply.voteGranted = false
		return
	}
	if rf.currentTerm < args.term {
		rf.currentTerm = args.term
		rf.status = Follower
		rf.votedFor = -1
	}
	reply.voteGranted = false
	reply.term = rf.currentTerm
	if(rf.votedFor == -1 || rf.votedFor == args.candidateId) && rf.isUpToDate(args.lastLogIndex, args.lastLogTerm) {
		reply.voteGranted = true;
		rf.granted <- true
		rf.votedFor = args.candidateId
	}
}
func (rf *Raft) isUpToDate(cIndex int, cTerm int) bool {
	term, index := rf.logs[len(rf.logs)-1].Term, len(rf.logs)-1

	if cTerm != term {
		return cTerm >= term
	}

	return cIndex >= index
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
	if !ok {
		return ok
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Candidate || rf.currentTerm != args.term {
		return ok
	}
	if reply.term > rf.currentTerm {
		rf.currentTerm = reply.term
		rf.status = Follower
		rf.votedFor = -1
		rf.persist()
		return ok
	}
	if reply.voteGranted {
		rf.voteCount++
		if rf.voteCount > len(rf.peers) /2 {
			rf.status = Leader
			rf.electWin <- true
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArg, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) selectLeader() {
	rf.mu.Lock()
	args := &RequestVoteArgs{}
	args.term = rf.currentTerm
	args.candidateId = rf.me
	args.lastLogIndex = rf.getLastIndex()
	args.lastLogTerm = rf.getLastTerm()
	defer rf.mu.Unlock()
	for server := range rf.peers {
		if server != rf.me && rf.status ==Candidate {
			go rf.sendRequestVote(server,args,&RequestVoteReply{})
		}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) runServer() {
	for {
		switch rf.status {
		case Leader：
			for i := range rf.peers {
				rf.nextIndex[i] = len(rf.logs)
				rf.matchIndex[i] = 0
			}
			go rf.;


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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.voteCount = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.status = Follower
	rf.applyCh = applyCh
	rf.electWin = make(chan bool)
	rf.granted = make(chan bool)
	rf.heartbeat = make(chan bool)
	rf.logs = append(rf.logs, LogEntry{Term: 0})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.runServer()
	return rf
}
