package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
    "time"
)

const Timeout = 10 * time.Millisecond

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Oprand string
    Args    interface{}
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    kvstore map[string]string
    cond *sync.Cond // linear serializability, Signal release one earliest request & idx satisfy requirement, like a queue
    progress int // current idx
    isLeader bool
    lastOp Op
    history map[int64]int
}


func (kv *RaftKV) DuplicateReq(cid int64, sid int) bool {
    value, ok := kv.history[cid]
    if !ok {
        return false
    } else {
        // return true
        return value >= sid
    }
}


func (kv *RaftKV) ReturnValue(key string, value string, op string) string {
    if op == "Put" {
        return value
    } else {
        return kv.kvstore[key] + value
    }
}

func (kv *RaftKV) Apply(args Op) {
    if args.Oprand == "Get" {
        // record history
        getArgs := args.Args.(GetArgs)
        kv.history[getArgs.Cid] = getArgs.Sid
    } else {
        putAppendArgs := args.Args.(PutAppendArgs)
        kv.history[putAppendArgs.Cid] = putAppendArgs.Sid
        kv.kvstore[putAppendArgs.Key] = kv.ReturnValue(putAppendArgs.Key, putAppendArgs.Value, args.Oprand)
    }
}


func (kv *RaftKV) Run(value Op, cid int64, sid int) bool {
    // wait till idx arrives at appropriate time
    idx, _, isLeader := kv.rf.Start(value)

    if isLeader {
        kv.cond.L.Lock()
        // corresponding to two Signal in make go func
        for kv.progress < idx && kv.isLeader {
            kv.cond.Wait()
        }
        kv.cond.L.Unlock()
    }

    if !kv.isLeader || kv.lastOp != value {
        isLeader = false
    }

    return isLeader
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    kv.mu.Lock()
    defer kv.mu.Unlock()

    value := Op{Oprand: "Get", Args: *args}
    ok := kv.Run(value, args.Cid, args.Sid)

    if !ok {
        reply.WrongLeader = true
    } else {
        reply.WrongLeader = false
        v, exist := kv.kvstore[args.Key]
        if exist {
            reply.Err = OK
            reply.Value = v
        } else {
            reply.Err = ErrNoKey
        }
    }
    return
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    kv.mu.Lock()
    defer kv.mu.Unlock()

    value := Op{Oprand: args.Op, Args: *args}
    ok := kv.Run(value, args.Cid, args.Sid)

    if !ok {
        reply.WrongLeader = true
    } else {
        reply.WrongLeader = false
        reply.Err = OK
    }
    return
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
// One server contains a serial of k-v pairs as stored locally
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
    gob.Register(PutAppendArgs{})
    gob.Register(GetArgs{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

    kv.history = make(map[int64]int)
    kv.kvstore = make(map[string]string)
    kv.progress = 0

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
    kv.cond = &sync.Cond{L: &sync.Mutex{}}

    go func() {
        for {
            // retrieve msg from channel
            applyMsg := <-kv.applyCh
            op := applyMsg.Command.(Op)

            var cid int64
            var sid int
            if op.Oprand == "Get" {
                getArgs := op.Args.(GetArgs)
                cid = getArgs.Cid
                sid = getArgs.Sid
            } else {
                putAppendArgs := op.Args.(PutAppendArgs)
                cid = putAppendArgs.Cid
                sid = putAppendArgs.Sid
            }

            // if request sent before, do not have to do again
            if !kv.DuplicateReq(cid, sid) {
                kv.Apply(op)
            }

            kv.cond.L.Lock()
            kv.progress = applyMsg.Index
            kv.lastOp = op
            kv.cond.L.Unlock()
            kv.cond.Signal()
        }
    }()

    go func() {
        for {
            _, isLeader := kv.rf.GetState()
            kv.cond.L.Lock()
            kv.isLeader = isLeader
            kv.cond.L.Unlock()
            kv.cond.Signal()
            time.Sleep(Timeout)
        }
    }()

	return kv
}
