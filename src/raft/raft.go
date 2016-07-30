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
    "sync"
    "labrpc"
    "time"
    "bytes"
    "encoding/gob"
    "math/rand"
)

const (
    Candidate = 0
    Follower  = 1
    Leader    = 2
)

const (
    Heartbeat = 100 * time.Millisecond
    Timeout = 150
)

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
//
//
type LogEntry struct {
    Index           int
    Command         interface{}
    Term            int
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
    // persistent states
    state           int // role of rf

    currentTerm     int // current term of given raft server
    votedForCandidate       int // index of candidate, could be nil (== -1 in our context)
    votedForTerm     int // term from candidate, necessary since votedForTerm != currentTerm
    log             []LogEntry // log storage for index, state machine already stored elsewhere

    // volatile states for all servers
    commitIndex     int // index of highest log entry known to be committed 
    lastApplied     int // index of highest log entry applied

    // volatile states for leader
    nextIndex       []int // 
    matchIndex      []int //

    // other necessary vars
    lastReceived    int64 // last time receive rpc from others

    applyCh         chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    // Your code here.
    term := rf.currentTerm
    isleader := rf.state == Leader

    return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
    // Your code here.
    w := new(bytes.Buffer)
    e := gob.NewEncoder(w)
    e.Encode(&rf.currentTerm)
    e.Encode(&rf.votedForCandidate)
    e.Encode(&rf.votedForTerm)
    e.Encode(&rf.log)
    data := w.Bytes()

    rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
    // Your code here.

    r := bytes.NewBuffer(data)
    d := gob.NewDecoder(r)
    d.Decode(&rf.currentTerm)
    d.Decode(&rf.votedForCandidate)
    d.Decode(&rf.votedForTerm)
    d.Decode(&rf.log)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
    // Your data here.
    Term            int
    CandidateId     int
    LastLogIndex    int
    LastLogTerm     int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
    // Your data here.
    Term            int
    VoteGranted     bool
}

// return last index of given rf.log 
func lastIdx(log []LogEntry) int {
    return log[len(log) - 1].Index
}

// return last term of given rf.log
func lastTerm(log []LogEntry) int {
    return log[len(log) - 1].Term
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
    // Your code here.
    // reply from server to candidate
    // only if raft serve has no leader || has leader that calls && candidate has most up to date log would grant true
    // from 5.1
    if (rf.currentTerm > args.Term) {
        reply.VoteGranted = false
        reply.Term = rf.currentTerm
        return
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()

    // state have to change if first condition not satified
    if (rf.currentTerm < args.Term) {
        rf.currentTerm = args.Term
        rf.state = Follower
    }

    uptodate := false
    // from 5.4
    // but adjust since "as uptodate as"
    if (lastTerm(rf.log) < args.LastLogTerm) {
        uptodate = true
    }
    if (lastTerm(rf.log) == args.LastLogTerm && lastIdx(rf.log) <= args.LastLogIndex) {
        uptodate = true
    }

    // 5.2 based on 5.4 uptodate flag
    granted := false
    if (rf.votedForTerm < args.Term || rf.votedForCandidate == args.CandidateId) && uptodate {
        granted = true
    }

    if granted {
        rf.votedForTerm = args.Term // rf would not have higher term than master if granted vote
        rf.votedForCandidate = args.CandidateId
        rf.lastReceived = time.Now().UnixNano()
    }

    // write to stable store before respond
    rf.persist()

    reply.VoteGranted = granted
    reply.Term = rf.currentTerm
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


type AppendEntriesArgs struct {
    Term            int // leader's term
    LeaderId        int
    PrevLogIndex    int
    PrevLogTerm     int
    Entries         []LogEntry
    LeaderCommit    int // leader commit index
}

type AppendEntriesReply struct {
    Term            int
    Success         bool
    NextIndex       int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
    // reply from server to candidate

    // 5.1
    if (rf.currentTerm > args.Term) {
        reply.Success = false
        reply.Term = rf.currentTerm
        reply.NextIndex = -1
        return
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()

    // 5.2 else 5.1
    rf.state = Follower
    rf.currentTerm = args.Term
    rf.lastReceived = time.Now().UnixNano()

    // 5.3 
    // len / lastIdx critical
    if args.PrevLogIndex >= len(rf.log) {
        rf.persist()
        reply.Success = false
        //reply.Term = rf.currentTerm
        reply.NextIndex = len(rf.log)
        return
    }

    // optimized necessary speed requirement
    if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
        nextIndex := args.PrevLogIndex

        for {
            if rf.log[nextIndex].Term != rf.log[args.PrevLogIndex].Term {
                break
            }
            nextIndex--
        }

        rf.persist()

        reply.Success = false
        reply.NextIndex = nextIndex + 1
        return
    }

    reply.Success = true

    // write 
    // conflict solution
    // dump / save log to persist store each time commit
    rf.log = rf.log[0:(args.PrevLogIndex+1)]

    for i := 0; i < len(args.Entries); i++ {
        if args.Entries[i].Index > lastIdx(rf.log) {
            rf.log = append(rf.log, args.Entries[i])
        }
    }

    // update commitIndex and ApplyMsg
    if args.LeaderCommit > rf.commitIndex {
        newcommitIdx := args.LeaderCommit
        if lastIdx(rf.log) < newcommitIdx {
            newcommitIdx = lastIdx(rf.log)
        }
        rf.commitIndex = newcommitIdx

        for i := rf.lastApplied + 1; i < rf.commitIndex + 1; i++ {
                msg := ApplyMsg{Index: i, Command: rf.log[i].Command}
                rf.applyCh <-msg
        }
        rf.lastApplied = rf.commitIndex
    }
    rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

//
// send heartbeats for leader
//
func (rf *Raft) SendHeartbeats() {
    for {

        rf.mu.Lock()
        li := lastIdx(rf.log)
        isLeader := rf.state == Leader
        rf.mu.Unlock()

        if !isLeader {
            return
        }

        for i := 0; i < len(rf.peers); i++ {
            if i != rf.me {
                go func(index int, peer int) {
                    args := AppendEntriesArgs{Term: rf.currentTerm, LeaderCommit: rf.commitIndex, LeaderId: rf.me}
                    args.PrevLogIndex = rf.nextIndex[peer] - 1
                    args.PrevLogTerm = rf.log[args.PrevLogIndex].Term

                    for i := rf.nextIndex[peer]; i < index + 1; i++ {
                        args.Entries = append(args.Entries, rf.log[i])
                    }

                    reply := AppendEntriesReply{}

                    ok := rf.sendAppendEntries(peer, args, &reply)

                    if !ok {
                        return
                    }

                    if reply.NextIndex == -1 {
                        rf.mu.Lock()
                        if reply.Term > rf.currentTerm {
                            rf.currentTerm = reply.Term
                        }

                        rf.state = Follower
                        rf.persist()

                        rf.mu.Unlock()
                        return
                    }

                    rf.mu.Lock()
                    if reply.Success {
                        rf.nextIndex[peer] = index + 1
                        rf.matchIndex[peer] = index
                    } else {
                        rf.nextIndex[peer] = reply.NextIndex
                    }

                    rf.mu.Unlock()
                }(li, i)
            }
        }

        rf.mu.Lock()

        // update commit by history records
        newcommitIdx := rf.commitIndex
        for i := rf.commitIndex + 1; i < len(rf.log); i++ {
            n := 1
            for j := 0; j < len(rf.peers); j++ {
                if j != rf.me && rf.matchIndex[j] >= i && rf.log[i].Term == rf.currentTerm {
                    n++
                }
            }

            if 2 * n > len(rf.peers) {
                newcommitIdx = i
            }
        }

        for i := rf.commitIndex + 1; i < newcommitIdx + 1; i++ {
            msg := ApplyMsg{Index: i, Command: rf.log[i].Command}
            rf.applyCh <-msg
        }

        rf.commitIndex = newcommitIdx

        rf.mu.Unlock()

        time.Sleep(Heartbeat)
    }
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
    isLeader := rf.state == Leader

    if !isLeader {
        return index, term, isLeader
    }

    index = lastIdx(rf.log) + 1
    entry := LogEntry{Command: command, Index: index, Term: term}
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
}

//
// start leader Election
// request votes from peers
func (rf *Raft) Election() {
    rf.mu.Lock()

    // increase cur tearm
    rf.currentTerm++
    rf.state = Candidate // change state

    rf.mu.Unlock()

    // default self 1
    nvotes := 1
    voteChan := make(chan RequestVoteReply, len(rf.peers))

    // send RequestVoteArgs
    for i := 0; i < len(rf.peers); i++ {
        if i != rf.me {
            go func(peer int){
                args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: lastIdx(rf.log), LastLogTerm: lastTerm(rf.log)}
                reply := RequestVoteReply{}

                ok := rf.sendRequestVote(peer, args, &reply)
                if !ok {
                    reply.VoteGranted = false
                }

                voteChan <-reply

            }(i)
        }
    }

    for i := 0; i < len(rf.peers)/2; i++ {
        v := <-voteChan
        if v.VoteGranted {
            nvotes++
        }
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()

    // if downgrade in between
    if rf.state == Follower {
        return
    }

    if (2 * nvotes > len(rf.peers)) {
        rf.state = Leader

        for i := 0; i < len(rf.peers); i++ {
            // reinit after election each time
            if i != rf.me {
                rf.nextIndex[i] = lastIdx(rf.log) + 1
                rf.matchIndex[i] = 0
            }
        }
        // heartbeats to all
        go rf.SendHeartbeats()
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

    // Your initialization code here.
    rf.currentTerm = 0
    rf.votedForCandidate = -1
    rf.votedForTerm = 0
    rf.log = append(rf.log, LogEntry{Index: 0, Term: 0}) // last valid entry idx = 1

    //
    rf.commitIndex = 0
    rf.lastApplied = 0

    //
    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))

    rf.state = Follower
    rf.lastReceived = time.Now().UnixNano()

    rf.applyCh = applyCh

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    // go routine for start voting, candidate state
    go func() {
        for {

            electionTimeout := int64(1e6 * (Timeout + rand.Intn(Timeout)))
            time.Sleep(time.Duration(electionTimeout))

            now := time.Now().UnixNano()

            rf.mu.Lock() // atomic operator
            reqElection := (now - rf.lastReceived) >= electionTimeout && rf.state != Leader
            rf.mu.Unlock()

            if (reqElection) {
                go rf.Election()
            }
        }
    }()

    return rf
}
