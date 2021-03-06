package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"fmt"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
	cid int64
	seq int
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
	ck.cid = nrand()
	ck.seq = 0
	ck.leader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{Key: key}
	for ; ;ck.leader=(ck.leader+1)%len(ck.servers) {
		reply := GetReply{}
		ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
		//fmt.Printf("leader is %d request status is %t. Wrong Leader ? %t \n", ck.leader,ok,reply.WrongLeader)
		if ok && !reply.WrongLeader {
			if reply.Err == ErrNoKey {
				return ""
			}

			return reply.Value
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seq++
	args := PutAppendArgs{Key: key, Value:value, Op:op, Cid: ck.cid, Seq:ck.seq}
	for ; ;ck.leader=(ck.leader+1)%len(ck.servers) {
		reply := PutAppendReply{}
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
		if args.Key == "c" || args.Key == "d"{
			fmt.Printf("leader %d is wrong leader? %t ok ? %t\n", ck.leader, reply.WrongLeader,ok)
		}
		if ok && !reply.WrongLeader {
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string){
	ck.PutAppend(key, value, "Put")

}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
