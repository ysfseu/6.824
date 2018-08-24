package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
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
	// otherwise RPC will break .
	Type string
	Key string
	Value string
	Cid int64
	Seq int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store	map[string]string   // key-value store
	request map[int64]int		// client cid to seq map for deduplication
	result	map[int]chan Op		// log index to Type chan map for checking if request succeeds

}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op {Type:"Get", Key: args.Key}
	ok := kv.sendOpToLog(op)
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	kv.mu.Lock()
	val, exist :=  kv.store[op.Key]
	kv.mu.Unlock()
	if !exist {
		reply.Err = ErrNoKey
	} else {
		reply.Value = val
		reply.Err = OK
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op {Type:args.Op, Key:args.Key, Value:args.Value, Cid:args.Cid, Seq:args.Seq}
	ok := kv.sendOpToLog(op)
	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	reply.Err = OK
}
func (kv *KVServer) sendOpToLog(op Op) bool {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	ch, ok := kv.result[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.result[index] = ch
	}
	kv.mu.Unlock()

	select {
	case cmd := <-ch:
		return cmd == op
	case <-time.After(800 * time.Millisecond):
		return false
	}
}


//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) apply() {
	for {
		msg := <- kv.applyCh
		op := msg.Command.(Op)
		//fmt.Printf("begin applying %s \n", op.Type)

		kv.mu.Lock()
		if op.Type != "Get" {
			if seq, ok := kv.request[op.Cid]; !ok || op.Seq > seq {
				if op.Type == "Put" {
					kv.store[op.Key] = op.Value
				}
				if op.Type == "Append" {
					kv.store[op.Key] += op.Value
				}
				kv.request[op.Cid] = op.Seq
			}
		}
		ch, ok := kv.result[msg.CommandIndex]

		if ok {
			ch <- op
		}
		kv.mu.Unlock()
		//fmt.Printf("Finish applying %s \n", op.Type)
	}



}
//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.store = make(map[string]string)   // key-value store
	kv.request = make(map[int64]int)		// client cid to seq map for deduplication
	kv.result = make(map[int]chan Op)
	go kv.apply()
	return kv
}
