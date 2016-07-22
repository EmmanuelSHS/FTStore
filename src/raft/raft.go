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
    Timeout = 150
    Heartbeat = 50
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
    State           interface{}
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
    isStopped       bool

    currentTerm     int // current term of given raft server
    votedFor        int // index of master, could be nil (== -1 in our context)
    log             []LogEntry // log storage for index, state machine already stored elsewhere

    // volatile states for all servers
    commitIndex     int // index of highest log entry known to be committed 
    lastApplied     int // index of highest log entry applied

    // volatile states for leader
    nextIndex       []int // 
    matchIndex      []int //

    // other necessary vars
    lastReceived    int64 // last time receive rpc from others
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
    e.Encode(&rf.votedFor)
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
    d.Decode(&rf.votedFor)
    d.Decode(&rf.log)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
    // Your data here.
    term            int
    candidateId     int
    lastLogIndex    int
    lastLogTerm     int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
    // Your data here.
    term            int
    voteGranted     bool
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
    rf.mu.Lock()
    defer rf.mu.Unlock()

    // from 5.1
    if (rf.currentTerm > args.term) {
        reply.voteGranted = false
        reply.term = rf.currentTerm
        return
    }

    // state have to change if first condition not satified
    if (rf.currentTerm < args.term) {
        rf.currentTerm = args.term
        rf.state = Follower
    }

    uptodate := false
    // from 5.4
    lt := lastTerm(rf.log)
    if (lt < args.lastLogTerm) {
        uptodate = true
    }
    if (lt == args.lastLogTerm && lastIdx(rf.log) < args.lastLogIndex) {
        uptodate = true
    }

    // 5.2 based on 5.4 uptodate flag
    granted := false
    if (rf.votedFor == -1 || rf.votedFor == args.candidateId) && uptodate {
        granted = true
        rf.currentTerm = args.term // rf would not have higher term than master if granted vote
        rf.votedFor = args.candidateId
        rf.lastReceived = time.Now().UnixNano()
    }

    // write to stable store before respond
    rf.persist()

    reply.voteGranted = granted
    reply.term = rf.currentTerm
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should probably
// pass &reply.
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
    term            int // leader's term
    leaderId        int
    prevLogIndex    int
    prevLogTerm     int
    entries         []int
    leaderCommit    int // leader commit index
}

type AppendEntriesReply struct {
    term            int
    success         bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
    // reply from server to candidate
    rf.mu.Lock()
    defer rf.mu.Unlock()

    // 5.1
    if (rf.currentTerm > args.term) {
        reply.success = false
        reply.term = rf.currentTerm
        return
    }

    // 5.2 else 5.1
    rf.state = Follower
    rf.votedFor = args.leaderId
    rf.currentTerm = args.term
    rf.lastReceived = time.Now().UnixNano()

    baseIdx := rf.log[0].Index // real idx starts not at 0
    // 5.3 
    if args.prevLogIndex > lastIdx(rf.log) || (args.prevLogIndex >= baseIdx && rf.log[args.prevLogIndex - baseIdx] != args.prevLogTerm) {
        rf.persist()
        reply.success = false
        reply.term = rf.currentTerm
        return
    }

    // write 
    // conflict solution
    // dump / save log to persist store each time commit
    if args.prevLogIndex < baseIdx {
        rf.log = rf.log[0:1]
    } else {
        rf.log = rf.log[0:(args.prevLogIndex - baseIdx + 1)]
        rf.log = append(rf.log, args.entries)
    }



}


func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

//
// send heartbeats for leader
//
func (rf *Raft) sendHeartbeats() {
    for {
        // make empty AppendEntries
        rf.mu.Lock()
        // guard
        if rf.state == Follower {
            rf.mu.Unlock()
            return
        }

        npeers := len(rf.peers)
        args := AppendEntriesArgs{}

        for i := 0; i < npeers; i++ {
            reply := AppendEntriesReply{}

            if i != rf.me {
                go func(peer int) {
                    rf.sendAppendEntries(peer, args, &reply)
                }(i)
            }
        }
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
    index := -1
    term := -1
    isLeader := true

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
// random time generator between duration
//
func randTime(min int, max int) int64 {
    return int64((max - min) * rand.Int() + min)
}

//
// start leader Election
// request votes from peers
func (rf *Raft) Election() {
    rf.mu.Lock()

    // increase cur tearm
    rf.currentTerm++
    rf.state = Candidate // change state
    rf.votedFor = rf.me

    // default self 1
    npeers := len(rf.peers)
    nvotes := 1
    voteChan := make(chan bool, npeers)

    rf.lastReceived = time.Now().UnixNano()
    args := RequestVoteArgs{term: rf.currentTerm, candidateId: rf.votedFor, 
                            lastLogIndex: lastIdx(rf.log), lastLogTerm: lastTerm(rf.log)}

    rf.mu.Unlock()

    // send RequestVoteArgs
    for i := 0; i < npeers; i++ {
        if i != rf.me {
            go func(peer int){
                reply := RequestVoteReply{}

                //sending & receving only from this peer
                ch := make(chan bool, 1)

                go func() {
                    ch <- rf.sendRequestVote(peer, args, &reply)
                }()

                // collect vote from this peer
                ok := false
                select {
                case ok = <-ch:
                case <- time.After(Timeout):
                    ok = false
                }

                if ok && reply.voteGranted {
                    //nvotes++ not possible, thread unsafe
                    voteChan <- true
                } else {
                    voteChan <- false
                }
            }(i)
        }
    }

    rf.mu.Lock()
    defer rf.mu.Lock()

    // if downgrade in between
    if rf.state == Follower {
        return
    }

    // count votes
    for v := range voteChan {
        if v {
            nvotes++
        }
    }

    if (nvotes >= npeers / 2 + 1) {
        rf.state = Leader

        li := lastIdx(rf.log)
        for i := 0; i < npeers; i++ {
            // reinit after election each time
            if i != rf.me {
                rf.nextIndex[i] = li + 1
                rf.matchIndex[i] = 0
            }
        }
        // heartbeats to all
        go rf.sendHeartbeats()
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
//
func Make(peers []*labrpc.ClientEnd, me int,
persister *Persister, applyCh chan ApplyMsg) *Raft {
    rf := &Raft{}
    rf.peers = peers
    rf.persister = persister
    rf.me = me

    // Your initialization code here.
    rf.currentTerm = 0
    rf.votedFor = -1
    rf.log = append(rf.log, LogEntry{Index: 0, Term: 0}) // last valid entry idx = 1

    //
    rf.commitIndex = 0
    rf.lastApplied = 0

    //
    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))

    rf.state = Follower
    rf.lastReceived = time.Now().UnixNano()

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    // go routine for start voting, candidate state
    go func() {
        for {
            electionTimeout := randTime(Timeout, 2 * Timeout)
            time.Sleep(time.Duration(electionTimeout))

            now := time.Now().UnixNano()

            rf.mu.Lock() // atomic operator
            reqElection := (now - rf.lastReceived) > electionTimeout && rf.state != Leader
            rf.mu.Unlock()

            if (reqElection) {
                go rf.Election()
            }
        }
    }()

    return rf
}
