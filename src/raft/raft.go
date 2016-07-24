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
    Heartbeat = 50 * time.Millisecond
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
    Command           interface{}
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

    applyCh         chan ApplyMsg
    done            chan bool
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
    //print("voter ")
    //print(rf.me)
    //print("\n")
    //print("currentTerm ")
    //print(rf.currentTerm)
    //print("\n")
    //print("len log ")
    //print(len(rf.log))
    //print("\n")
    //print("args candidate ")
    //print(args.CandidateId)
    //print("args term ")
    //print(args.Term)
    //print("\n")
    // from 5.1
    if (rf.currentTerm > args.Term) {
        //print("in direct return")
        reply.VoteGranted = false
        reply.Term = rf.currentTerm
        return
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()

    // state have to change if first condition not satified
    if (rf.currentTerm < args.Term) {
        //print("in args term higher\n")
        rf.currentTerm = args.Term
        rf.state = Follower
    }

    uptodate := false
    // from 5.4
    // but adjust since "as uptodate as"
    if (lastTerm(rf.log) <= args.LastLogTerm) {
        uptodate = true
    }
    if (lastTerm(rf.log) == args.LastLogTerm && lastIdx(rf.log) <= args.LastLogIndex) {
        uptodate = true
    }
    //print("update ")
    //print(uptodate)
    //print("\n")

    // 5.2 based on 5.4 uptodate flag
    granted := false
    if (rf.currentTerm <= args.Term || rf.votedFor == args.CandidateId) && uptodate {
        //print("in vote granted\n")
        granted = true
        rf.currentTerm = args.Term // rf would not have higher term than master if granted vote
        rf.votedFor = args.CandidateId
        rf.lastReceived = time.Now().UnixNano()
    }

    // write to stable store before respond
    rf.persist()

    reply.VoteGranted = granted
    reply.Term = rf.currentTerm

    //print("relpy ")
    //print(reply.Term)
    //print(" ")
    //print(reply.VoteGranted)
    //print("\n")
    //return
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
    rf.mu.Lock()
    defer rf.mu.Unlock()

    // 5.1
    if (rf.currentTerm > args.Term) {
        reply.Success = false
        reply.Term = rf.currentTerm
        reply.NextIndex = -1
        return
    }

    // 5.2 else 5.1
    rf.state = Follower
    rf.votedFor = args.LeaderId
    rf.currentTerm = args.Term
    rf.lastReceived = time.Now().UnixNano()

    baseIdx := rf.log[0].Index // real idx starts not at 0

    // 5.3 
    if args.PrevLogIndex > lastIdx(rf.log) {
        rf.persist()
        reply.Success = false
        //reply.Term = rf.currentTerm
        reply.NextIndex = lastIdx(rf.log) + 1
        return
    }

    if (args.PrevLogIndex >= baseIdx && rf.log[args.PrevLogIndex - baseIdx].Term != args.PrevLogTerm) {
        // optimized necessary speed requirement
        nextIndex := args.PrevLogIndex

        for {
            if rf.log[nextIndex - baseIdx].Term != rf.log[args.PrevLogIndex - baseIdx].Term {
                break
            }
            nextIndex--
        }

        rf.persist()

        reply.Success = false
        reply.NextIndex = nextIndex + 1
        return
    }

    // write 
    // conflict solution
    // dump / save log to persist store each time commit
    rf.log = rf.log[0:args.PrevLogIndex+1]

    for i := 0; i < len(args.Entries); i++ {
        if args.Entries[i].Index > lastIdx(rf.log) {
            rf.log = append(rf.log, args.Entries[i])
        }
    }

    reply.Success = true
    reply.Term = rf.currentTerm
    reply.NextIndex = lastIdx(rf.log) + 1

    // update commitIndex and ApplyMsg
    if args.LeaderCommit > rf.commitIndex {
        rf.commitIndex = args.LeaderCommit

        if lastIdx(rf.log) < rf.commitIndex {
            rf.commitIndex = lastIdx(rf.log)
        }

        for i := rf.lastApplied + 1; i < rf.commitIndex + 1; i++ {
            if i - baseIdx > 0 {
                msg := ApplyMsg{Index: i, Command: rf.log[i - baseIdx].Command}
                rf.applyCh <-msg
            }
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
/*
func (rf *Raft) SendHeartbeats() {
    for {
        if rf.shutdown() {
            //print("sendddddd heartbeatssssss shutdown\n")
            return
        }

        // make empty AppendEntries
        rf.mu.Lock()
        //print("sending heartbeats")
        // guard
        if rf.state != Leader {
            //print("becomesss follllller againnnnnnnn\n")
            rf.mu.Unlock()
            return
        }

        //print("1\n")
        baseIdx := rf.log[0].Index
        //print("2\n")
        // update commit by history records
        newcommitIdx := rf.commitIndex
        //print("3\n")
        for i := rf.commitIndex + 1; i < lastIdx(rf.log) + 1; i++ {
            //print(i)
            //print("\n")
            if rf.log[i - baseIdx].Term == rf.currentTerm {
                n := 1
                for j := 0; j < len(rf.peers); j++ {
                    if j != rf.me && rf.matchIndex[j] >= i {
                        n++
                    }
                }

                if 2 * n > len(rf.peers) {
                    newcommitIdx = i
                }
            }
        }

        //print("4\n")
        // commit via applyCh
        for i := rf.commitIndex + 1; i < newcommitIdx + 1; i++ {
            msg := ApplyMsg{Index: i, Command: rf.log[i - baseIdx].Command}
            rf.applyCh <-msg
        }

        rf.commitIndex = newcommitIdx
        rf.lastApplied = newcommitIdx


        // prepare args to send
        args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, LeaderCommit: rf.commitIndex}

        //print("5\n")
        // send & handle failure
        for i := 0; i < len(rf.peers); i++ {
            if i != rf.me {
                if rf.nextIndex[i] > baseIdx { // necessary guard
                    //print("7\n")
                    // fill rest args
                    args.PrevLogIndex = rf.nextIndex[i] - 1
                    args.PrevLogTerm = rf.log[args.PrevLogIndex - baseIdx].Term
                    args.Entries = make([]LogEntry, len(rf.log[rf.nextIndex[i] - baseIdx:]))
                    copy(args.Entries, rf.log[rf.nextIndex[i] - baseIdx:])

                    go func(peer int) {
                        reply := AppendEntriesReply{}
                        //print("append entries args ")
                        //print(args.Term)
                        //print(" ")
                        //print(args.LeaderId)
                        //print(" ")
                        //print(args.LeaderCommit)
                        //print (" to peer ")
                        //print(peer)
                        //print(" \n")

                        ok := rf.sendAppendEntries(peer, args, &reply)
                        //print("peer ")
                        //print(peer)
                        //print(" replied ")
                        //print(reply.Term)
                        //print(" ")
                        //print(reply.Success)
                        //print(" ok ")
                        //print(ok)
                        //print("\n")

                        if !ok {
                            return
                        }

                        rf.mu.Lock()
                        defer rf.mu.Unlock()

                        if rf.state != Leader {
                            return
                        }

                        if args.Term != rf.currentTerm {
                            return
                        }

                        if reply.Term > args.Term {
                            rf.currentTerm = reply.Term
                            rf.state = Follower
                            rf.persist()
                            return
                        }

                        if reply.Success {
                            if len(args.Entries) > 0 {
                                rf.nextIndex[peer] = lastIdx(args.Entries) + 1
                                rf.matchIndex[peer] = rf.nextIndex[peer] - 1
                            } else {
                                // optimized
                                rf.nextIndex[peer] = reply.NextIndex
                            }
                        }
                    }(i)
                }
            }
        }
        //print("6\n")
        rf.mu.Unlock()

        time.Sleep(Heartbeat)
    }
}
*/
func (rf *Raft) SendHeartbeats() {
    for {
        li := lastIdx(rf.log)

        if rf.state == Leader {
            return
        }

        rf.mu.Lock()

        //print("1\n")
        baseIdx := rf.log[0].Index
        //print("2\n")
        // update commit by history records
        newcommitIdx := rf.commitIndex
        //print("3\n")
        for i := rf.commitIndex + 1; i < lastIdx(rf.log) + 1; i++ {
            //print(i)
            //print("\n")
            if rf.log[i - baseIdx].Term == rf.currentTerm {
                n := 1
                for j := 0; j < len(rf.peers); j++ {
                    if j != rf.me && rf.matchIndex[j] >= i {
                        n++
                    }
                }

                if 2 * n > len(rf.peers) {
                    newcommitIdx = i
                }
            }
        }

        rf.commitIndex = newcommitIdx
        rf.lastApplied = newcommitIdx

        rf.mu.Unlock()

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
    close(rf.done)
}

//
func (rf *Raft) shutdown() bool {
    select {
        case <-rf.done:
            return true
        default:
            return false
    }
}

//
// start leader Election
// request votes from peers
func (rf *Raft) Election() {
    rf.mu.Lock()

    //print("candidate ")
    //print(rf.me)
    //print("\n")
    // increase cur tearm
    rf.currentTerm++
    //print("votee term")
    //print(rf.currentTerm)
    //print("\n")
    rf.state = Candidate // change state
    rf.votedFor = rf.me

    // default self 1
    npeers := len(rf.peers)
    nvotes := 1
    voteChan := make(chan bool, npeers)

    rf.lastReceived = time.Now().UnixNano()
    args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: lastIdx(rf.log), LastLogTerm: lastTerm(rf.log)}

    rf.mu.Unlock()

    // send RequestVoteArgs
    for i := 0; i < npeers; i++ {
        if i != rf.me {
            go func(peer int){
                //print("send to ")
                //print(peer)
                //print("\n")
                reply := RequestVoteReply{}

                ok := rf.sendRequestVote(peer, args, &reply)
                //sending & receving only from this peer
                //ch := make(chan bool, 1)
                //ok := false

                //go func() {
                //    //print("votee send vote args term ")
                //    //print(args.Term)
                //    //print("\n")
                //    ch <- rf.sendRequestVote(peer, args, &reply)
                //}()

                //// collect vote from this peer
                //select {
                //case ok = <-ch:
                //// tunning correct wait-for time is necessary
                //case <- time.After(100 * time.Millisecond):
                //    ok = false
                //}

                //print("peer reply returned ")
                //print(peer)
                //print(" ok ")
                //print(ok)
                //print(" vote ")
                //print(reply.VoteGranted)
                //print("\n")
                if ok && reply.VoteGranted {
                    //nvotes++ not possible, thread unsafe
                    //print("int true chan\n")
                    voteChan <- true
                } else {
                    //print("int false chan\n")
                    voteChan <- false
                }
                //print("reply sent back ")
                //print(peer)
                //print(reply.Term)
                //print(reply.VoteGranted)
                //print("\n")
            }(i)
        }
    }

    //print("the votes collected as ")
    //print(nvotes)
    //print("\n")

    rf.mu.Lock()
    defer rf.mu.Unlock()

    // if downgrade in between
    if rf.state == Follower {
        return
    }

    // count votes
    for i := 0; i < npeers - 1; i++ {
        v := <-voteChan
        //print("from vote chan ")
        //print(v)
        //print("\n")
        if v {
            nvotes++
        }
    }

    if (2 * nvotes > npeers) {
        rf.state = Leader
        //print("becomes leader rrrrrr \n")
        //print(rf.state)

        li := lastIdx(rf.log)
        for i := 0; i < npeers; i++ {
            // reinit after election each time
            //print("election ")
            //print(i)
            //print("\n")
            if i != rf.me {
                //print("here\n")
                rf.nextIndex[i] = li + 1
                //print("there\n")
                rf.matchIndex[i] = 0
            }
        }
        //print("out\n")
        // heartbeats to all
        //print("sending heartbeatsssssssssssss\n")
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

    rf.applyCh = applyCh
    rf.done = make(chan bool)

    // initialize from state persisted before a crash
    rf.mu.Lock()
    rf.readPersist(persister.ReadRaftState())
    rf.mu.Unlock()

    // go routine for start voting, candidate state
    go func() {
        for {
            if rf.shutdown() {
                return
            }

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
