package shardctrler

import "time"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num int // config number
	// 0 1 2 3 1 2 1 0 1 0
	Shards [NShards]int // shard -> gid
	// 0:x y z 1:a b c 2:t f g
	Groups map[int][]string // gid -> servers[]
}

const (
	OK         = "OK"
	ErrTimeout = "ErrTimeout"
	ErrUnknown = "ErrUnknown"
)
const (
	AGREE_TIMEOUT = 100 * time.Millisecond
)
const (
	JOIN  = 1
	LEAVE = 2
	QUERY = 3
	MOVE  = 4
)
const (
	EMPTY_GID = 0
)

type Err string

type JoinArgs struct {
	ClientID  int
	RequestID int
	Servers   map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs      []int
	ClientID  int
	RequestID int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard     int
	GID       int
	ClientID  int
	RequestID int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num       int // desired config number
	ClientID  int
	RequestID int
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
