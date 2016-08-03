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
import "labrpc"
import "time"
import "math/rand"
import "bytes"
import "encoding/gob"

const (
	Leader            = 0
	Candidate         = 1
	Follower          = 2
	HeartBeatInterval = 50 * time.Millisecond // 50ms
)

// LogEntry

type LogEntry struct {
	Index   int
	Command interface{}
	Term    int
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan ApplyMsg

	// Persistent state on all servers
	currentTerm       int
	votedForCandidate int
	votedForTerm      int
	log               []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	role        int // leader, candidate, follower
	lastContact int64

	done chan bool
}

func (rf *Raft) shutdown() bool {
	select {
	case <-rf.done:
		return true
	default:
		return false
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedForCandidate)
	e.Encode(rf.votedForTerm)
	e.Encode(rf.log)
	data := w.Bytes()

	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readSnapshot(data []byte) {

	rf.readPersist(rf.persister.ReadRaftState())

	if len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var LastIncludedIndex int
	var LastIncludedTerm int

	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)

	rf.commitIndex = LastIncludedIndex
	rf.lastApplied = LastIncludedIndex

	rf.log = truncateLog(LastIncludedIndex, LastIncludedTerm, rf.log)

	msg := ApplyMsg{UseSnapshot: true, Snapshot: data}

	go func() {
		rf.applyCh <- msg
	}()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	if len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedForCandidate)
	d.Decode(&rf.votedForTerm)
	d.Decode(&rf.log)
}

func LastIndex(log []LogEntry) int {
	return log[len(log)-1].Index
}

func LastTerm(log []LogEntry) int {
	return log[len(log)-1].Term
}

func (rf *Raft) SendHeartBeat() {

	for {

		if rf.shutdown() {
			return
		}

		rf.mu.Lock()

		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}

		baseIndex := rf.log[0].Index

		// update commitIndex

		N := rf.commitIndex

		for i := rf.commitIndex + 1; i <= LastIndex(rf.log); i++ {
			num := 1
			for j := 0; j < len(rf.peers); j++ {

				if j != rf.me && rf.matchIndex[j] >= i && rf.log[i-baseIndex].Term == rf.currentTerm {
					num++
				}
			}
			if 2*num > len(rf.peers) {
				N = i
			}
		}

		for i := rf.commitIndex + 1; i <= N; i++ {
			msg := ApplyMsg{Index: i, Command: rf.log[i-baseIndex].Command}
			rf.applyCh <- msg
		}

		rf.commitIndex = N
		rf.lastApplied = N

		// send log entries

		appendArgs := AppendEntriesArgs{Term: rf.currentTerm, LeaderCommit: rf.commitIndex, LeaderId: rf.me}
		installArgs := InstallSnapshotArgs{Term: rf.currentTerm, LeaderId: rf.me, LastIncludedIndex: rf.log[0].Index, LastIncludedTerm: rf.log[0].Term, Data: rf.persister.snapshot}

		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				if rf.nextIndex[i] > baseIndex {

					appendArgs.PrevLogIndex = rf.nextIndex[i] - 1
					appendArgs.PrevLogTerm = rf.log[appendArgs.PrevLogIndex-baseIndex].Term
					appendArgs.Entries = make([]LogEntry, len(rf.log[rf.nextIndex[i]-baseIndex:]))
					copy(appendArgs.Entries, rf.log[rf.nextIndex[i]-baseIndex:])

					go func(args AppendEntriesArgs, server int) {

						reply := &AppendEntriesReply{}

						ok := rf.sendAppendEntries(server, args, reply)

						if !ok {
							return
						}

						rf.mu.Lock()
						defer rf.mu.Unlock()

						if rf.role != Leader {
							return
						}

						if args.Term != rf.currentTerm {
							return
						}

						if reply.Term > args.Term { // followerTerm is larger
							rf.currentTerm = reply.Term
							rf.role = Follower
							rf.persist()
							return
						}

						if reply.Success {
							if len(args.Entries) > 0 {
								rf.nextIndex[server] = LastIndex(args.Entries) + 1
								rf.matchIndex[server] = LastIndex(args.Entries)
							}
						} else {
							rf.nextIndex[server] = reply.NextIndex
						}

					}(appendArgs, i)
				} else {
					go func(args InstallSnapshotArgs, server int) {
						reply := &InstallSnapshotReply{}

						ok := rf.sendInstallSnapshot(server, args, reply)

						if !ok {
							return
						}

						rf.mu.Lock()
						defer rf.mu.Unlock()

						if reply.Term > rf.currentTerm { // followerTerm is larger
							rf.currentTerm = reply.Term
							rf.role = Follower
							rf.persist()
							return
						}

						// installing succeed

						rf.nextIndex[server] = args.LastIncludedIndex + 1
						rf.matchIndex[server] = args.LastIncludedIndex

					}(installArgs, i)
				}
			}
		}

		rf.mu.Unlock()

		time.Sleep(HeartBeatInterval)
	}

}

func (rf *Raft) StartElection() {

	//t1:= time.Now()

	rf.mu.Lock()

	// increment currentTerm
	rf.currentTerm++
	// change to Candidate
	rf.role = Candidate

	// reset election timer
	rf.lastContact = time.Now().UnixNano()

	// vote for self, need to check if I can vote for myself
	numVotes := 1

	rf.votedForTerm = rf.currentTerm
	rf.votedForCandidate = rf.me

	// Send RequestVote RPCs to all other servers

	votesChan := make(chan bool, len(rf.peers))
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.me, LastLogIndex: LastIndex(rf.log), LastLogTerm: LastTerm(rf.log)}

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {

			go func(peer int) {

				reply := RequestVoteReply{}

				ch := make(chan bool, 1)
				ok := false

				go func() {
					ch <- rf.sendRequestVote(peer, args, &reply)
				}()

				select {
				case ok = <-ch:
				case <-time.After(100 * time.Millisecond):
					ok = false
				}

				if ok && reply.VoteGranted {
					votesChan <- true
				} else {
					votesChan <- false
				}
			}(i)
		}
	}

	for i := 0; i < len(rf.peers)-1; i++ {
		vote := <-votesChan
		if vote {
			numVotes++
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == Follower {
		return
	}

	if numVotes*2 > len(rf.peers) {
		rf.role = Leader

		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				rf.nextIndex[i] = LastIndex(rf.log) + 1
				rf.matchIndex[i] = 0
			}
		}

		// thread to send heartBeat

		go rf.SendHeartBeat()
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.role == Leader
	// Your code here.

	return term, isleader
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
	}

	// 5.4

	uptoDate := false

	if args.LastLogTerm > LastTerm(rf.log) {
		uptoDate = true
	}

	if args.LastLogTerm == LastTerm(rf.log) && args.LastLogIndex >= LastIndex(rf.log) { // at least up to date
		uptoDate = true
	}

	// 5.2
	grantVote := false

	if (rf.votedForTerm < args.Term || rf.votedForCandidate == args.CandidateID) && uptoDate { // think more here
		grantVote = true
	}

	if grantVote {
		rf.votedForTerm = args.Term
		rf.votedForCandidate = args.CandidateID
		rf.lastContact = time.Now().UnixNano()
	}

	rf.persist()

	reply.VoteGranted = grantVote
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// changle role to Follower

	rf.role = Follower
	rf.currentTerm = args.Term
	rf.lastContact = time.Now().UnixNano()

	baseIndex := rf.log[0].Index

	if args.PrevLogIndex > LastIndex(rf.log) {
		rf.persist()
		reply.Success = false
		reply.NextIndex = LastIndex(rf.log) + 1
		return
	}

	if args.PrevLogIndex >= baseIndex && args.PrevLogTerm != rf.log[args.PrevLogIndex-baseIndex].Term {

		// optimization

		nextIndex := args.PrevLogIndex

		for {
			if nextIndex-baseIndex < 0 {
				nextIndex = baseIndex
				break
			}
			if rf.log[nextIndex-baseIndex].Term != rf.log[args.PrevLogIndex-baseIndex].Term {
				break
			}
			nextIndex--
		}

		rf.persist()

		reply.Success = false
		reply.NextIndex = nextIndex + 1
		return
	}

	// erase conflicts entries

	if args.PrevLogIndex < baseIndex { // must equal due to commit

		rf.log = rf.log[0:1]

		for i := 0; i < len(args.Entries); i++ {
			if args.Entries[i].Index > rf.log[0].Index {
				rf.log = append(rf.log, args.Entries[i])
			}
		}

	} else {
		rf.log = rf.log[0 : args.PrevLogIndex-baseIndex+1]

		for i := 0; i < len(args.Entries); i++ {
			rf.log = append(rf.log, args.Entries[i])
		}
	}

	reply.Success = true
	reply.NextIndex = LastIndex(rf.log) + 1
	// update commitIndex and applyMsg

	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := args.LeaderCommit

		if LastIndex(rf.log) < newCommitIndex {
			newCommitIndex = LastIndex(rf.log)
		}

		rf.commitIndex = newCommitIndex
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			if i-baseIndex > 0 {
				msg := ApplyMsg{Index: i, Command: rf.log[i-baseIndex].Command}
				rf.applyCh <- msg
			}
		}

		rf.lastApplied = rf.commitIndex
	}

	rf.persist()
}

func truncateLog(lastIncludedIndex int, lastIncludedTerm int, log []LogEntry) []LogEntry {

	var newLogEntries []LogEntry
	newLogEntries = append(newLogEntries, LogEntry{Index: lastIncludedIndex, Term: lastIncludedTerm})

	for index := len(log) - 1; index >= 0; index-- {
		if log[index].Index == lastIncludedIndex && log[index].Term == lastIncludedTerm {
			newLogEntries = append(newLogEntries, log[index+1:]...)
			break
		}
	}

	return newLogEntries
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// changle role to Follower

	rf.role = Follower
	rf.currentTerm = args.Term
	rf.lastContact = time.Now().UnixNano()

	// step 2 ~ 5

	rf.persister.SaveSnapshot(args.Data)
	// step 6, 7

	rf.log = truncateLog(args.LastIncludedIndex, args.LastIncludedTerm, rf.log)

	// step 8

	msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}

	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	rf.persist()

	rf.applyCh <- msg

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.role == Leader

	if !isLeader {
		return index, term, isLeader
	}

	index = LastIndex(rf.log) + 1

	var entry LogEntry

	entry.Command = command
	entry.Index = index
	entry.Term = term

	rf.log = append(rf.log, entry)

	rf.persist()

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
	close(rf.done)
}

func (rf *Raft) GetPersistSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) StartSnapshot(snapshot []byte, index int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].Index
	lastIndex := LastIndex(rf.log)

	if index <= baseIndex || index > lastIndex {
		// in case having installed a snapshot from leader before snapshotting
		// second condition is a hack
		return
	}

	var newLogEntries []LogEntry

	newLogEntries = append(newLogEntries, LogEntry{Index: index, Term: rf.log[index-baseIndex].Term})

	for i := index + 1; i <= lastIndex; i++ {
		newLogEntries = append(newLogEntries, rf.log[i-baseIndex])
	}

	rf.log = newLogEntries

	rf.persist()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(newLogEntries[0].Index)
	e.Encode(newLogEntries[0].Term)

	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveSnapshot(data)

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

	// Your initialization code here.

	rf.applyCh = applyCh
	rf.done = make(chan bool)

	// Persistent state on all servers

	rf.currentTerm = 0
	rf.votedForTerm = 0
	rf.votedForCandidate = -1
	rf.log = append(rf.log, LogEntry{Index: 0, Term: 0}) // index starts from 1

	// Volatile state on all servers

	rf.commitIndex = 0
	rf.lastApplied = 0

	// Volatile state on leaders

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.role = Follower
	rf.lastContact = time.Now().UnixNano()

	// initialize from state persisted before a crash

	rf.mu.Lock()

	rf.readSnapshot(persister.ReadSnapshot())

	rf.mu.Unlock()

	// thread to start a new election

	go func() {
		for {

			if rf.shutdown() {
				return
			}

			electionTimeout := int64(1000000 * (150 + rand.Intn(150))) // [150, 300)

			time.Sleep(time.Duration(electionTimeout))

			now := time.Now().UnixNano()
			rf.mu.Lock()

			elect := now-rf.lastContact >= electionTimeout && rf.role != Leader

			rf.mu.Unlock()

			if elect {
				go rf.StartElection()
			}
		}
	}()

	return rf
}
