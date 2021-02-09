package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

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

type entry struct {
	Term    int
	Commond string
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type appendEntryReply struct {
	Term int
	Rst  bool
}

type appendEntry struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      entry
	LeaderCommit int
}

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

	State        int // 0 leader, 1 follower, 2 candidate
	CurrentTerm  int
	peersNum     int
	FollowersNum int
	LastLogIndex int
	LastLogTerm  int
	//serversNum                 int
	NextIndex                   []int
	Voted                       int
	Log                         []entry
	LastBeatHeartTime           int64
	ElectionTimeout             int64
	ElectionTimeOutCheckChannel chan bool
	AppendEntryChannel          chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	// isleader = if rf.State==0
	if rf.State == 0 {
		isleader = true
	} else {
		isleader = false
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
	CandidateNum  int
	CandidataTerm int
	LastLogIndex  int
	LastLogTerm   int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoteResult  bool
	FollowerNum int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.FollowerNum = rf.me
	if rf.Voted != -1 {
		if args.CandidataTerm >= rf.CurrentTerm {
			if args.LastLogTerm >= rf.LastLogTerm {
				if args.LastLogIndex >= rf.LastLogIndex {
					reply.VoteResult = true
					rf.Voted = args.CandidateNum
					return
				}
			}
		}
	}

	reply.VoteResult = false
	return
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
// term. the third return value is/*  */ true if this server believes it is
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

func (rf *Raft) eventloop() bool {
	fmt.Printf("%d begin eventloop\n",rf.me)
	for {
		select {
		case <-rf.ElectionTimeOutCheckChannel:
			//fmt.Printf("Election Time out\n")
			rf.convert2Candidate()
			voteRequest := RequestVoteArgs{rf.me, rf.CurrentTerm, 0, 0}
			var voteReply RequestVoteReply
			var wg sync.WaitGroup
			for i := 0; i < rf.peersNum-1; i++ {
				wg.Add(1)
				go func(i int) {
					if i == rf.me {
						return
					} else {
						rst := rf.sendRequestVote(i, &voteRequest, &voteReply)

						fmt.Printf("send request vote to %d\n", i)

						if rst == true {
							if voteReply.VoteResult == true {
								rf.mu.Lock()
								rf.FollowersNum += 1
								rf.mu.Unlock()
							}
						}
					}
					wg.Done()
				}(i)

			}
			wg.Wait()
			if rf.FollowersNum >= rf.peersNum/2 {
				rf.convert2Leader()

			}

		case <-rf.AppendEntryChannel:

		}
	}

}

// My code

func (rf *Raft) electionTimeOutCheck() {
	go func() {
		for {
			now := time.Now().UnixNano()
			elaspe := (now - rf.LastBeatHeartTime) / int64(time.Millisecond)
			//fmt.Println(elaspe, rf.ElectionTimeout)
			if elaspe > rf.ElectionTimeout {
				//fmt.Printf("elasp > rf.ElectionTimeout\n")
				if rf.State == 0 || rf.State==2 { //leader

				} else {
					rf.ElectionTimeOutCheckChannel <- true

				}
			}
		}

	}()

}

func (rf *Raft) sendAppendEntry(server int, args *appendEntry, reply *appendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.EntryReceive", args, reply)
	return ok
}

func (rf *Raft) EntryReceive(args *appendEntry, reply *appendEntryReply) {
	now := time.Now().UnixNano()
	rf.LastBeatHeartTime = int64(now)
	if args.term < rf.CurrentTerm {
		reply.term = rf.CurrentTerm
		reply.rst = false
		return
	} else {
		reply.term = rf.CurrentTerm
		reply.rst = true
	}

	//TODO :判断数据是否一致

}

func (rf *Raft) broadBeat() {
	for {
		rf.mu.Lock()
		term, isleader := rf.GetState()
		rf.mu.Unlock()
		if isleader == false {
			return
		}
		//TODO: get laster appendEntry info

		args := appendEntry{term: term,
			leaderId:     rf.me,
			prevLogIndex: 0,
			prevLogTerm:  0,
			entries:      rf.Log[0],
			leaderCommit: rf.LastLogIndex,
		}

		var reply appendEntryReply

		for i := 0; i < rf.peersNum; i++ {
			if i == rf.me {
				continue
			}
			go func() {
				rst := rf.sendAppendEntry(i, &args, &reply)
				if rst == false {
					//TODO: peer failed
				} else {
					//TODO: 判断对端term是否大于自己的term
					// 如果 结果为 false，则转换为follower

					if reply.rst != true {
						if reply.term > rf.CurrentTerm {
							rf.convert2Follower()
							return
						} else {
							//TODO: 重新发送appendEntry
						}
					}
				}

			}()
		}

	}
}

func setElectionTimeout() int64 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	randomTime := r.Intn(150) + 150
	return int64(randomTime)
}

func (rf *Raft) initialization() {

	rf.State = 1 //follower
	rf.CurrentTerm = 0
	// rf.NextIndex
	rf.Voted = -1
	// rf.Log
	rf.LastBeatHeartTime = time.Now().UnixNano()
	rf.ElectionTimeout = setElectionTimeout()           //TODO: add a function to create random time
	rf.ElectionTimeOutCheckChannel = make(chan bool, 1) // TODO : I don't know  if the channel should have buffer
	rf.peersNum = len(rf.peers)
	rf.FollowersNum = 0

	go rf.eventloop()
}

func (rf *Raft) convert2Leader() {
	if rf.State == 1 {
		return
	}
	rf.State = 0
	rf.CurrentTerm++
	rf.Voted = -1
	rf.LastBeatHeartTime = time.Now().UnixNano()
	rf.ElectionTimeout = setElectionTimeout()
}

func (rf *Raft) convert2Candidate() {
	rf.CurrentTerm += 1
	rf.State = 2
	rf.Voted = -1
	rf.LastBeatHeartTime = time.Now().UnixNano()
	rf.ElectionTimeout = setElectionTimeout()
	rf.FollowersNum = 0
}

func (rf *Raft) convert2Follower() {

}

//

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
	rf.initialization()
	rf.electionTimeOutCheck()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
