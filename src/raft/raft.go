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
	"time"
	"math/rand"
	"bytes"
	"labgob"
)


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
	commited chan bool


}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == Leader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int //candidate’s term
	CandidateId int //candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry (§5.4)
	LastLogTerm int //term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

type AppendEntriesArg struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	NextTryIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextTryIndex =rf.getLastIndex() +1
		return
	}
	rf.heartbeat <- true
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
	}
	reply.Term = args.Term
	if args.PrevLogIndex > rf.getLastIndex()   {
		reply.NextTryIndex =rf.getLastIndex() + 1
		return
	}
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		term := rf.logs[args.PrevLogIndex].Term
		for reply.NextTryIndex = args.PrevLogIndex -1; reply.NextTryIndex > 0; reply.NextTryIndex--{
			if rf.logs[reply.NextTryIndex].Term != term {
				break
			}
		}
		reply.NextTryIndex++
		return
	}
	rf.logs = rf.logs[:args.PrevLogIndex+1]
	rf.logs = append(rf.logs, args.Entries...)
	reply.Success = true
	reply.NextTryIndex = rf.getLastIndex()+1
	if args.LeaderCommit > rf.commitIndex {
		last := rf.getLastIndex()
		if args.LeaderCommit > last {
			rf.commitIndex = last
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.commited <- true;
	}
	return


}
func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getLastTerm() int {
	return rf.logs[len(rf.logs) - 1].Term
}

func (rf *Raft) convertToFollower(term int) {
	defer rf.persist()
	DPrintf("Convert server(%v) state(%v=>follower) term(%v => %v)", rf.me,
		rf.status, rf.currentTerm, term)
	rf.status = Follower
	rf.currentTerm = term
	rf.votedFor = -1
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
	}
	if(rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true;
		rf.votedFor = args.CandidateId
		rf.status = Follower
		rf.granted <- true
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
	if rf.status != Candidate || rf.currentTerm != args.Term {
		return ok
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.persist()
		return ok
	}
	if reply.VoteGranted {
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
	if !ok {
		return ok
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if ok {
		if rf.status != Leader {
			return ok
		}
		if args.Term != rf.currentTerm {
			return ok
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.status = Follower
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = reply.NextTryIndex
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			rf.nextIndex[server] = reply.NextTryIndex
		}
	}
	return ok
}

func (rf *Raft) selectLeader() {
	rf.mu.Lock()
	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastIndex()
	args.LastLogTerm = rf.getLastTerm()
	rf.mu.Unlock()
	for server := range rf.peers {
		if server != rf.me && rf.status ==Candidate {
			go rf.sendRequestVote(server,args,&RequestVoteReply{})
		}
	}
}
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for peer := range rf.peers {
		if peer!= rf.me && rf.status == Leader {
			args := &AppendEntriesArg{}
			args.Term = rf.currentTerm
			args.PrevLogIndex = rf.getLastIndex()
			args.PrevLogTerm = rf.getLastTerm()
			args.LeaderCommit = rf.commitIndex
			if args.PrevLogIndex >= 0 {
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			}
			if rf.nextIndex[peer] <= rf.getLastIndex() {
				args.Entries = rf.logs[rf.nextIndex[peer]:]
			}
			go rf.sendAppendEntries(peer, args, &AppendEntriesReply{})
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
	//fmt.Printf("%d begin start.................................................", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("%d get lock............................", rf.me)
	index := -1
	term := -1
	isLeader := rf.status == Leader
	if isLeader {
		term  = rf.currentTerm
		rf.logs = append(rf.logs, LogEntry{Term: term, Command:command})
		rf.persist()
		//fmt.Printf("%d add command succeeded.................................................", rf.me)
	}

	return index, term, isLeader
}
func (rf *Raft) commitLogs() {
	for {
		select {
		case <- rf.commited:
			rf.mu.Lock()
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				if rf.currentTerm != rf.logs[i].Term {
					break;
				}
				rf.applyCh <- ApplyMsg{CommandIndex: i, Command: rf.logs[i].Command}
				rf.lastApplied = i
			}
			rf.mu.Unlock()
		}
	}


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
		case Leader:
			//fmt.Printf("Begin broadcast entries %d\n", rf.me)
			rf.broadcastAppendEntries()
			time.Sleep(time.Millisecond * 120)
		case Follower:
			//fmt.Printf("%d is follower\n", rf.me)
			select {
				case <- rf.granted:
				case <- rf.heartbeat:
				case <-time.After(time.Millisecond * time.Duration(rand.Intn(200) + 300)):
					rf.mu.Lock()
					rf.status = Candidate
					rf.mu.Unlock()
					//fmt.Printf("%d follower -> candidate\n", rf.me)
			}
		case Candidate:
			//fmt.Printf("%d is candidate\n", rf.me)
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.persist()
			rf.voteCount = 1
			rf.mu.Unlock()
			go rf.selectLeader()
			select {
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(200)+300)):
			case <-rf.heartbeat:
			case <-rf.granted:
				rf.mu.Lock()
				rf.status = Follower
				rf.mu.Unlock()
				//fmt.Printf("%d candidate -> follwer\n", rf.me)
			case <-rf.electWin:
				rf.mu.Lock()
				rf.status = Leader
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				nextIdx := rf.getLastIndex() + 1
				for i := range rf.peers {
					rf.nextIndex[i] = nextIdx
					rf.matchIndex[i] = 0
				}
				rf.mu.Unlock()
				//fmt.Printf("%d candidate -> leader\n", rf.me)
			}
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
	rf.commited = make(chan bool)
	rf.logs = append(rf.logs, LogEntry{Term: 0})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.runServer()
	go rf.commitLogs()
	return rf
}
