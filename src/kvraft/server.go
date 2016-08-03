package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
    "time"
    "bytes"
)

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

type DBEntry struct {
    Command Op
    Exist bool
    Value string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    kvstore map[string]string
    history map[int64]int
    result map[int]chan DBEntry
    done chan bool
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

func (kv *RaftKV) CheckSnapshot(idx int) {
    if kv.maxraftstate != -1 && float64(kv.rf.GetPersistSize()) > float64(kv.maxraftstate) * 0.8 {
        w := new(bytes.Buffer)
        e := gob.NewEncoder(w)
        e.Encode(kv.kvstore)
        e.Encode(kv.history)
        data := w.Bytes()

        go kv.rf.StartSnapshot(data, idx)
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


func (kv *RaftKV) Run(value Op) (bool, DBEntry) {
    // wait till idx arrives at appropriate time
    var entry DBEntry
    idx, _, isLeader := kv.rf.Start(value)

    if !isLeader {
        return false, entry
    }

    kv.mu.Lock()

    ch, exist := kv.result[idx]

    if !exist {
        kv.result[idx] = make(chan DBEntry, 1)
        ch = kv.result[idx]
    }

    kv.mu.Unlock()

    select {
    case entry = <-ch:
        return entry.Command == value, entry
    case <-time.After(1000 * time.Millisecond):
        return false, entry
    }
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    kv.mu.Lock()
    defer kv.mu.Unlock()

    value := Op{Oprand: "Get", Args: *args}
    ok, entry := kv.Run(value)

    if !ok {
        reply.WrongLeader = true
    } else {
        reply.WrongLeader = false
        if entry.Exist {
            reply.Err = OK
            reply.Value = entry.Value
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
    ok, _ := kv.Run(value)

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

func (kv *RaftKV) shutdown() bool {
    select {
    case <-kv.done:
        return true
    default:
        return false
    }
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
    kv.done = make(chan bool)

    kv.history = make(map[int64]int)
    kv.kvstore = make(map[string]string)
    kv.result = make(map[int]chan DBEntry)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

    go func() {
        for {

            if kv.shutdown() {
                return
            }
            // retrieve msg from channel
            applyMsg := <-kv.applyCh

            if applyMsg.UseSnapshot {
                var LastIncludedIndex int
                var LastIncludedTerm int

				r := bytes.NewBuffer(applyMsg.Snapshot)
				d := gob.NewDecoder(r)

				kv.mu.Lock()
				d.Decode(&LastIncludedIndex)
				d.Decode(&LastIncludedTerm)
				kv.kvstore = make(map[string]string)
				kv.history = make(map[int64]int)
				d.Decode(&kv.kvstore)
				d.Decode(&kv.history)
				kv.mu.Unlock()
			} else {

				op := applyMsg.Command.(Op)

				var machine int64
				var req int
				var key string
				var entry DBEntry
				index := applyMsg.Index

				if op.Oprand == "Get" {
					getArgs := op.Args.(GetArgs)
					machine = getArgs.Cid
					req = getArgs.Sid
					key = getArgs.Key
				} else {
					putAppendArgs := op.Args.(PutAppendArgs)
					machine = putAppendArgs.Cid
					req = putAppendArgs.Sid
				}

				kv.mu.Lock()

				if !kv.DuplicateReq(machine, req) {
					kv.Apply(op)
				}

				entry.Command = op

				if op.Oprand == "Get" {
					v, exist := kv.kvstore[key]
					entry.Value = v
					entry.Exist = exist
				}

				_, exist := kv.result[index]

				if !exist {
					kv.result[index] = make(chan DBEntry, 1)
				} else {
					// clear the channel
					select {
					case <-kv.result[index]:
					default:
					}
				}

				kv.result[index] <- entry
				kv.CheckSnapshot(applyMsg.Index)
				kv.mu.Unlock()

			}
		}
	}()

	return kv
}
