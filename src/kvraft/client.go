package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"


const (
    Wait = 100 * time.Millisecond
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
    sid int // serial number
    id int64 // clerk id

}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
    sid := 0
    id := nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
    args := &GetArgs{Key: key, Cid: ck.id, Sid: ck.sid}
    ck.sid++

    // outer for to make sure iteration till leader elected
    for {
        for _, server := range ck.servers {
            reply := &GetReply{}
            ok := server.Call("RaftKV.Get", args, reply)

            if !reply.WrongLeader && ok {
                if reply.Err == ErrNoKey {
                    reply.Value = ""
                }

                return reply.Value
            }
        }

        time.sleep(Wait)
    }
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
    args := &PutAppendArgs{Key: key, Value: value, Op: op, Cid: ck.id, Sid: ck.sid}
    ck.sid++

    for {
        for _, server := range ck.servers {
            reply := &PutAppendReply{}
            ok := server.Call("RaftKV.PutAppend", &args, &reply)

            if !reply.WrongLeader && ok {
                return
            }
        }
        time.sleep(Wait)
    }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
