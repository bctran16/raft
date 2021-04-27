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
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"bytes"
	"6.824/labgob"
)

// import "bytes"
// import "6.824/labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogTerm  int // term of candidate's last log entry
	LastLogIndex int //	term of candidate's last index
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
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
	Heartbeat    bool
}
type AppendEntriesReply struct {
	Term             int
	Success          bool
	Server           int
	EntriesLastIndex int
	Heartbeat bool
}

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Data []byte
}

type InstallSnapshotReply struct  {
	Term int
	Server int
	LastIncludedIndex int
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type State struct {
	term     int
	isleader bool
}

type StartReply struct {
	term     int
	index    int
	isLeader bool
}

const (
	Follower  = iota
	Candidate = iota
	Leader    = iota
)

const SHOULD_LOG = true

const HeartbeatIntervalsMs = 100
const ElectionTimeoutMinsMs = 500
const ElectionTimeoutMaxMs = 1000

//function that samples random time
func SampleElectionTimeout() time.Duration {
	duration := ElectionTimeoutMaxMs - ElectionTimeoutMinsMs
	timeoutMs := ElectionTimeoutMinsMs + rand.Int31n(int32(duration))
	return time.Duration(timeoutMs)
}

type RequestVoteRpc struct {
	args    *RequestVoteArgs
	replyCh chan RequestVoteReply
}

type AppendEntriesRpc struct {
	args    *AppendEntriesArgs
	replyCh chan AppendEntriesReply
}

type InstallSnapshotRpc struct {
	args *InstallSnapshotArgs
	replyCh chan InstallSnapshotReply
}

type SnapshotCommand struct {
	index int
	snapshot []byte
}
type StartCommand struct {
	command interface{}
	replyCh chan StartReply
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	term        int
	vote        int          // whoever the server votes for
	role        int          // Follower, Candiate, Leader
	commitIndex int          // highest index in log entry committed
	lastApplied int          // highest index in log entry of state machine
	logger      *log.Logger  // log for each server
	log         []LogEntry   // log
	timer       *time.Ticker // timer to keep track of timeout
	// leaders only attributes
	nextIndex  []int // index of the next log entry to send to a server
	matchIndex []int // index of higest log entry known to be replicated on server
	lastIncludedIndex int
	lastIncludedTerm int
	// channels
	voteReceived         int // number of votes received
	requestVoteCh        chan RequestVoteRpc
	requestVoteReplyCh   chan RequestVoteReply // channel to read request vote reply
	appendEntriesCh      chan AppendEntriesRpc
	appendEntriesReplyCh chan AppendEntriesReply // channel to read append entries reply
	installSnapshotCh chan InstallSnapshotRpc
	installSnapshotReplyCh chan InstallSnapshotReply // channel to read snapshot reply
	getStateCh           chan chan State         // channel to get state
	startCh              chan StartCommand
	heartbeatCh          chan bool
	applyCh              chan ApplyMsg // channel to apply message
	snapshotCh chan SnapshotCommand
}

func (rf *Raft) sendHeartbeat() {
	for rf.killed() == false {
		_, isleader := rf.GetState()
		if isleader {
			rf.heartbeatCh <- true
			time.Sleep(HeartbeatIntervalsMs * time.Millisecond)
		} else {
			break // stop go routine when the leader is no longer alive
		}
	}
}

// Central Operation Loop
func (rf *Raft) loop() {
	for {
		select {
		case <-rf.heartbeatCh:
			{
				snapshot:= rf.persister.ReadSnapshot()
				startIndex:= rf.lastIncludedIndex
				lastLogIndex:=rf.lastIncludedIndex
				// making sure that log is not 0 
				if len(rf.log)>0 {
					startIndex = rf.log[0].Index
					lastLogIndex = rf.log[len(rf.log)-1].Index
				}

				for i := range rf.peers {
					if i != rf.me {
						if rf.nextIndex[i]-1==lastLogIndex {
							// follower's and leader's log are the same
							log := make([]LogEntry, 0)
							prevLogIndex := rf.lastIncludedIndex
							prevLogTerm := rf.lastIncludedTerm
							if len(rf.log)>0 {
								prevLogIndex = rf.nextIndex[i] - 1 // 0 for first iteration
								prevLogTerm = rf.log[prevLogIndex-startIndex].Term // 0 for first iteration
							}
							args := AppendEntriesArgs{rf.term, rf.me, prevLogIndex, prevLogTerm, log, rf.commitIndex, true}
							reply := AppendEntriesReply{}
							go rf.sendAppendEntries(i, &args, &reply)
						} else if rf.nextIndex[i]>= startIndex && len(rf.log)>0{
							// next index is still in the the log, so catch up by sending entries
							args:=AppendEntriesArgs{}
							args.Term = rf.term
							args.LeaderId = rf.me
							args.PrevLogIndex = rf.nextIndex[i] - 1
							args.LeaderCommit = rf.commitIndex
							args.Heartbeat = false
							if args.PrevLogIndex>=startIndex {
								args.PrevLogTerm = rf.log[args.PrevLogIndex-startIndex].Term // 0 for first iteration
							} else {
								args.PrevLogTerm = rf.lastIncludedTerm
							}
							if rf.nextIndex[i]<=rf.log[len(rf.log)-1].Index {
								args.Entries = rf.log[rf.nextIndex[i]-startIndex:]
							}
							reply := AppendEntriesReply{}
							go rf.sendAppendEntries(i, &args, &reply)
						} else {
							// send snapshot when index lags behind
							args:= &InstallSnapshotArgs{}
							args.Term = rf.term
							args.LeaderId = rf.me
							lastIncludeTerm, lastIncludedIndex, data := decode(snapshot)
							args.LastIncludedIndex = lastIncludedIndex
							args.LastIncludedTerm = lastIncludeTerm
							args.Data = data
							reply := &InstallSnapshotReply{}
							go rf.sendInstallSnapshot(i, args, reply)
						}
					}
				}
			}
		case requestVoteRPC := <-rf.requestVoteCh:
			{
				requestVoteReply := RequestVoteReply{}
				if requestVoteRPC.args.Term < rf.term {
					// when the candidate term is fewer
					requestVoteReply.Term = rf.term
					requestVoteReply.VoteGranted = false
					requestVoteRPC.replyCh <- requestVoteReply
				} else {
					// when the candidate term is larger, become follower and update term
					if requestVoteRPC.args.Term > rf.term {
						rf.role = Follower
						rf.term = requestVoteRPC.args.Term
						rf.vote = -1
						rf.persist()
					}
					requestVoteReply.Term = rf.term
					requestVoteReply.VoteGranted = false
					// cases that vote are granted
					if (rf.vote == -1 || rf.vote == requestVoteRPC.args.CandidateId) && rf.UpToDate(requestVoteRPC.args.LastLogTerm, requestVoteRPC.args.LastLogIndex) {
						rf.vote = requestVoteRPC.args.CandidateId
						requestVoteReply.VoteGranted = true
						rf.timer.Stop()
						duration := SampleElectionTimeout()
						rf.timer.Reset(duration * time.Millisecond) // set new election time out
						rf.persist()
					}
					requestVoteRPC.replyCh <- requestVoteReply
				}

			}
		// Channel that handles request vote reply
		case requestVoteReply := <-rf.requestVoteReplyCh:
			{
				if requestVoteReply.VoteGranted && requestVoteReply.Term == rf.term {
					// vote granted
					rf.voteReceived += 1
					if rf.voteReceived > (len(rf.peers)/2) && rf.role != Leader {
						// become leaders after getting majority and isn't already a leader
						rf.role = Leader
						//initialize next index and match index whenever a new leader is elected
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						for i := range rf.nextIndex {
							if len(rf.log)==0 {
								rf.nextIndex[i] = rf.lastIncludedIndex +1
							} else {
								rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
							}
							rf.matchIndex[i] = 0
						}
						go rf.sendHeartbeat()
					}
				} else {
					// vote not granted
					if requestVoteReply.Term > rf.term {
						// become a follower if term is less
						rf.role = Follower
						rf.term = requestVoteReply.Term
						rf.persist()

					}
				}
			}
		// Channel that handles appendEntries
		case appendEntriesRPC := <-rf.appendEntriesCh:
			{
				appendEntriesReply := AppendEntriesReply{}
				appendEntriesReply.Heartbeat = appendEntriesRPC.args.Heartbeat
				appendEntriesReply.Server = rf.me
				appendEntriesReply.Success = false
				if !appendEntriesRPC.args.Heartbeat {
					appendEntriesReply.EntriesLastIndex = appendEntriesRPC.args.Entries[len(appendEntriesRPC.args.Entries)-1].Index
				}
				if appendEntriesRPC.args.Term > rf.term {
					// if leader term is larger, revert back to follower per rules for all server
					rf.role = Follower
					rf.term = appendEntriesRPC.args.Term
					rf.vote = -1
					rf.persist()
				} else if appendEntriesRPC.args.Term < rf.term {
					// if term is larger, remain the same and return immediately per append entries receiver implementation 1.
					appendEntriesReply.Term = rf.term
				} else {
					rf.timer.Stop()
					duration := SampleElectionTimeout()
					rf.timer.Reset(duration * time.Millisecond) // start new timer
					appendEntriesReply.Term = rf.term
					if len(rf.log)>0 {
						// case where log is not empty
						startIndex := rf.log[0].Index
						
						// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
						if appendEntriesRPC.args.PrevLogIndex > rf.log[len(rf.log)-1].Index {
							continue;
						} else if appendEntriesRPC.args.PrevLogIndex>= startIndex && rf.log[appendEntriesRPC.args.PrevLogIndex-startIndex].Term != appendEntriesRPC.args.PrevLogTerm {
							continue;
						} else if appendEntriesRPC.args.PrevLogIndex>= startIndex-1 {
							// case when the log up to prevLogIndex is correct
							
							// if existing entry conflicts with a new one, delete all that follows it 
							// append entries that have not been added
							if appendEntriesRPC.args.PrevLogIndex== rf.log[len(rf.log)-1].Index {
								// all entries from appendEntries because previous log index and log length is the same
								//rf.logger.Printf("PrevLogIndex and last log index agree %d\n",appendEntriesRPC.args.Entries)
								rf.log = append(rf.log, appendEntriesRPC.args.Entries...)
							} else {
								// prev log index is smaller than the log
								logIndex := appendEntriesRPC.args.PrevLogIndex + 1 -startIndex
								for i, entry := range appendEntriesRPC.args.Entries {
									// check if log already exists
									if logIndex<=(len(rf.log)-1) {
										// case when entries might overlap
										logEntry:= rf.log[logIndex]
										if logEntry.Term==entry.Term && logEntry.Command == entry.Command && logEntry.Index==entry.Index {
											// entry already existed. Do not append
											logIndex++
										} else if logEntry.Index==entry.Index && logEntry.Term!=entry.Term {
											// log and entries doesn't agree, delete existing entry and all that follows. append new entry
											rf.log = rf.log[:entry.Index-startIndex]
											rf.log = append(rf.log, entry)
											logIndex++
										}
									} else {
										// case where the rest of the entries are new. append all
										entries:= appendEntriesRPC.args.Entries[i:]
										rf.log = append(rf.log, entries...)
										break
									}
								}
							}
							appendEntriesReply.Success = true
							rf.persist()
							// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
							if appendEntriesRPC.args.LeaderCommit > rf.commitIndex {
								rf.commitIndex = min(appendEntriesRPC.args.LeaderCommit, rf.log[len(rf.log)-1].Index)
								rf.applyLog()
							}
						}
					} else {
						// case where log is empty since it just received a snapshot
						// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
						if appendEntriesRPC.args.PrevLogIndex > rf.lastIncludedIndex {
							continue;
						} else if rf.lastIncludedTerm != appendEntriesRPC.args.PrevLogTerm {
							continue;
						} else {
							// case when the log up to prevLogIndex is correct

							// if existing entry conflicts with a new one, delete all that follows it 
							// append entries that have not been added
							
							if appendEntriesRPC.args.PrevLogIndex== rf.lastIncludedIndex {
								// all entries from appendEntries because previous log index and log length is the same
								rf.log = append(rf.log, appendEntriesRPC.args.Entries...)
							}
							appendEntriesReply.Success = true
							rf.persist()
							// appendEntriesReply.NextIndex = appendEntriesRPC.args.PrevLogIndex + len(appendEntriesRPC.args.Entries)
							// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
							if appendEntriesRPC.args.LeaderCommit > rf.commitIndex {
								rf.commitIndex = min(appendEntriesRPC.args.LeaderCommit, rf.log[len(rf.log)-1].Index)
								rf.applyLog()
							}
						}
					}

				}

				// If an existing entry conflicts with a new one (same index
				// but different terms), delete the existing entry and all that
				//follow it (§5.3)
				appendEntriesRPC.replyCh <- appendEntriesReply
			}
		// Channel that Handles Append Entries Reply
		case appendEntriesReply := <-rf.appendEntriesReplyCh:
			{
				if appendEntriesReply.Term > rf.term {
					rf.role = Follower
					rf.term = appendEntriesReply.Term
					rf.vote = -1
					rf.persist()

				}

				if appendEntriesReply.Success && !appendEntriesReply.Heartbeat && rf.role == Leader{
					rf.nextIndex[appendEntriesReply.Server] = appendEntriesReply.EntriesLastIndex + 1
					rf.matchIndex[appendEntriesReply.Server] = rf.nextIndex[appendEntriesReply.Server] - 1
				} else if !appendEntriesReply.Success && rf.role == Leader {
					if rf.nextIndex[appendEntriesReply.Server] > 1 {
						rf.nextIndex[appendEntriesReply.Server] -=1
					}
				}
				// Checking reply to commit entry
				if rf.role == Leader && len(rf.log)>0{
					startIndex:= rf.log[0].Index
					for N := rf.log[len(rf.log)-1].Index; N > rf.commitIndex && rf.log[N-startIndex].Term == rf.term; N-- {
						count := 1
						for i := range rf.peers {
							if i != rf.me && rf.matchIndex[i] >= N {
								count++
							}
						}
						if count > len(rf.peers)/2 {
							rf.commitIndex = N
							rf.applyLog()
							break
						}
					}
				}
				

			}

		case installSnapshotRPC := <-rf.installSnapshotCh: {
			installSnapshotReply := InstallSnapshotReply{}
			installSnapshotReply.Server = rf.me
			
			if installSnapshotRPC.args.Term > rf.term {
				// if leader term is larger, revert back to follower per rules for all server
				rf.role = Follower
				rf.term = installSnapshotRPC.args.Term
				rf.vote = -1
				rf.persist()
			} else if installSnapshotRPC.args.Term < rf.term {
				// if term is larger, remain the same and return immediately per append entries receiver implementation 1.
				installSnapshotReply.Term = rf.term
			} else {
				// reset timer
				rf.timer.Stop()
				duration := SampleElectionTimeout()
				rf.timer.Reset(duration * time.Millisecond) // start new timer
				installSnapshotReply.Term = rf.term
				
				if installSnapshotRPC.args.LastIncludedIndex > rf.commitIndex {
					// case where the leader's snapshot is ahead of follower's snapshot
					installSnapshotReply.LastIncludedIndex = installSnapshotRPC.args.LastIncludedIndex
					rf.trimLog(installSnapshotRPC.args.LastIncludedIndex, installSnapshotRPC.args.LastIncludedTerm)
					rf.lastApplied = installSnapshotRPC.args.LastIncludedIndex
					rf.commitIndex = installSnapshotRPC.args.LastIncludedIndex
					rf.lastIncludedIndex = installSnapshotRPC.args.LastIncludedIndex
					rf.lastIncludedTerm = installSnapshotRPC.args.LastIncludedTerm
					msg:= ApplyMsg{SnapshotValid: true, Snapshot: installSnapshotRPC.args.Data, SnapshotTerm: installSnapshotRPC.args.LastIncludedTerm, SnapshotIndex: installSnapshotRPC.args.LastIncludedIndex}
					rf.applyCh <- msg
					rf.takeSnapshot(installSnapshotRPC.args.Data)
				} else {
					installSnapshotReply.LastIncludedIndex = rf.commitIndex
				}
			}
			installSnapshotRPC.replyCh <- installSnapshotReply
		}
		case installSnapshotReply:= <-rf.installSnapshotReplyCh: {
			if installSnapshotReply.Term > rf.term {
				// becomes follower and update term
				rf.term = installSnapshotReply.Term
				rf.role = Follower
				rf.vote = -1
				rf.persist()
			}
			rf.nextIndex[installSnapshotReply.Server] = installSnapshotReply.LastIncludedIndex + 1 
			rf.matchIndex[installSnapshotReply.Server] = installSnapshotReply.LastIncludedIndex
		}
		case getStateReplyCh := <-rf.getStateCh:
			{
				// get state channel to prevent data race
				state := State{rf.term, rf.role == Leader}
				getStateReplyCh <- state
			}
		case startCommand := <-rf.startCh:
			{

				startReply := StartReply{}
				if rf.role == Leader {
					// adding commands to the log as a leader
					startReply.term = rf.term
					startReply.index = rf.lastIncludedIndex +1 
					if len(rf.log)>0 {
						startReply.index = rf.log[len(rf.log)-1].Index + 1
					}
					startReply.isLeader = true
					rf.log = append(rf.log, LogEntry{Index: startReply.index, Term: startReply.term, Command: startCommand.command})
					rf.persist()
				} else {
					startReply.term = -1
					startReply.index = -1
					startReply.isLeader = false
				}
				startCommand.replyCh <- startReply
			}

		case snapshotCommand:= <- rf.snapshotCh:
		{
			if rf.log[len(rf.log)-1].Index==snapshotCommand.index {
				// All of logs included in snapshot. Wipe log
				startIndex:=rf.log[0].Index
				rf.lastIncludedIndex = snapshotCommand.index
				rf.lastIncludedTerm = rf.log[rf.lastIncludedIndex-startIndex].Term
				rf.log=rf.log[len(rf.log)-1:]
			} else {
				// some elements of log still remain
				startIndex:=rf.log[0].Index
				for _, logEntry := range rf.log {
					if snapshotCommand.index == logEntry.Index {
						rf.lastIncludedIndex = snapshotCommand.index
						rf.lastIncludedTerm = logEntry.Term
						// if index ==i, trim everything from 0 to i. keep elements from i + 1
						rf.log = rf.log[snapshotCommand.index+1-startIndex:]
						break
					}
				}
			}
			rf.takeSnapshot(snapshotCommand.snapshot)
		}
		case <-rf.timer.C:
			{
				// start election when timer runs out
				if rf.role != Leader && rf.killed() == false {
					rf.role = Candidate
					rf.vote = rf.me // vote for itself
					rf.voteReceived = 1
					rf.term += 1
					rf.persist()
					rf.timer.Stop()
					duration := SampleElectionTimeout()
					rf.timer.Reset(duration * time.Millisecond) // set new election time out
					for i := range rf.peers {
						if i != rf.me {
							args := RequestVoteArgs{}
							args.Term = rf.term
							args.CandidateId = rf.me
							args.LastLogIndex = rf.lastIncludedIndex
							args.LastLogTerm = rf.lastIncludedTerm
							if len(rf.log)>0 {
								args.LastLogIndex = rf.log[len(rf.log)-1].Index
								args.LastLogTerm = rf.log[len(rf.log)-1].Term
							}
							reply := RequestVoteReply{}
							go rf.sendRequestVote(i, &args, &reply)
						}
					}
				}
			}

		}
	}
}

func (rf*Raft) takeSnapshot(snapshot [] byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(snapshot)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(rf.GetRaftState(), data)

}

func (rf*Raft) reinstallSnapshot (snapshot []byte){
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	var lastIncludedIndex, lastIncludedTerm int
	var data []byte 
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&lastIncludedIndex)
	d.Decode(&lastIncludedTerm)
	d.Decode(&data)
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastIncludedIndex = lastIncludedIndex
	rf.trimLog(rf.lastIncludedIndex, rf.lastIncludedTerm)
	msg:= ApplyMsg{SnapshotValid: true, Snapshot: data, SnapshotTerm: rf.lastIncludedTerm, SnapshotIndex: rf.lastIncludedIndex}
	rf.applyCh <- msg
}

// function to check if the candidate's log is at least up to date with the follower log.
func (rf *Raft) UpToDate(lastLogTerm int, lastLogIndex int) bool {
	
	serverLastTerm := rf.lastIncludedTerm
	serverLastIndex := rf.lastIncludedIndex
	if len(rf.log)>0 {
		serverLastTerm = rf.log[len(rf.log)-1].Term   // server's last log term
		serverLastIndex = rf.log[len(rf.log)-1].Index // server's last log index
	}
	return lastLogTerm > serverLastTerm || (lastLogTerm == serverLastTerm && lastLogIndex >= serverLastIndex)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	replyCh := make(chan State)
	rf.getStateCh <- replyCh
	res := <-replyCh
	return res.term, res.isleader
}

func (rf *Raft) GetRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.vote)
	e.Encode(rf.log)
	return w.Bytes()
}

func (rf *Raft) trimLog(lastIncludedIndex int, lastIncludedTerm int) {
	newLog :=make([]LogEntry, 0)
	for i:= len(rf.log) -1; i>=0; i-- {
		if rf.log[i].Index == lastIncludedIndex && rf.log[i].Term == lastIncludedTerm {
			newLog = append(newLog, rf.log[i+1:]...)
			break
		}
	}
	rf.log = newLog
}
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.vote)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) applyLog() {
	startIndex:= rf.log[0].Index
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{}
		if i == 0 {
			msg.CommandValid = false
		} else {
			msg.CommandValid = true
		}
		msg.CommandIndex = i
		msg.Command = rf.log[i-startIndex].Command
		rf.applyCh <- msg
	}
	rf.lastApplied = rf.commitIndex
}

func min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func decode (snapshot []byte) (int,int,[]byte) {

	var lastIncludedIndex, lastIncludedTerm int
	var data []byte 
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&lastIncludedIndex)
	d.Decode(&lastIncludedTerm)
	d.Decode(&data)
	return lastIncludedTerm, lastIncludedIndex, data

}
//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { 
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int 
	var vote int 
	var log []LogEntry
	if d.Decode(&term) != nil ||
	   d.Decode(&vote) != nil ||
	   d.Decode(&log) != nil  {
	} else {
	  rf.term = term
	  rf.vote = vote
	  rf.log = log
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// trim log until index
	snapshotCommand := SnapshotCommand{index, snapshot}
	rf.snapshotCh <- snapshotCommand
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	replyCh := make(chan RequestVoteReply)
	requestVoteRpc := RequestVoteRpc{args, replyCh}
	rf.requestVoteCh <- requestVoteRpc
	rep := <-replyCh
	*reply = rep
}

// AppendEntries Handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	replyCh := make(chan AppendEntriesReply)
	appendEntriesRpc := AppendEntriesRpc{args, replyCh}
	rf.appendEntriesCh <- appendEntriesRpc
	*reply = <-replyCh
}

// InstallSnapshot Handler
func (rf*Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	replyCh := make(chan InstallSnapshotReply)
	installSnapshotRpc := InstallSnapshotRpc {args,replyCh}
	rf.installSnapshotCh <- installSnapshotRpc
	*reply = <- replyCh
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	term, _ := rf.GetState()
	if ok {
		if term != reply.Term {
			return ok
			// invalid vote
		}
		rf.requestVoteReplyCh <- *reply
	}

	// send to countvotechannel
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	term, isleader := rf.GetState()
	if !ok || !isleader || term != args.Term {
		return ok
	}
	rf.appendEntriesReplyCh <- *reply

	return ok
}

func (rf*Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok:= rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	term, isleader :=rf.GetState()
	if !ok || !isleader || term != args.Term {
		return ok
	}
	rf.installSnapshotReplyCh <- *reply
	return ok
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	startCh := make(chan StartReply)
	startRequest := StartCommand{command, startCh}
	rf.startCh <- startRequest
	startReply := <-startCh

	return startReply.index, startReply.term, startReply.isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	// initialization
	rf := &Raft{}
	// initialize log
	if SHOULD_LOG {
		rf.logger = log.New(os.Stderr, fmt.Sprintf("peer id %v: ", me), log.Ldate|log.Lmicroseconds|log.Lshortfile|log.Lmsgprefix)
	} else {
		rf.logger = log.New(ioutil.Discard, "", 0)
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.role = Follower
	rf.term = 0
	rf.vote = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0, Index: 0})
	rf.voteReceived = 0

	//volatile states
	rf.lastApplied = -1
	rf.commitIndex = -1

	// channels for serialization
	rf.requestVoteCh = make(chan RequestVoteRpc, 100)
	rf.requestVoteReplyCh = make(chan RequestVoteReply, 100)
	rf.getStateCh = make(chan chan State, 100)
	rf.appendEntriesCh = make(chan AppendEntriesRpc, 100)
	rf.appendEntriesReplyCh = make(chan AppendEntriesReply, 100)
	rf.installSnapshotCh = make(chan InstallSnapshotRpc, 100)
	rf.installSnapshotReplyCh = make (chan InstallSnapshotReply, 100)
	rf.snapshotCh = make (chan SnapshotCommand, 100)
	rf.startCh = make(chan StartCommand, 100)
	rf.heartbeatCh = make(chan bool, 100)
	rf.applyCh = applyCh
	duration := SampleElectionTimeout()
	rf.timer = time.NewTicker(duration * time.Millisecond)

	rf.readPersist(persister.ReadRaftState())
	
	// recovering from failure
	go func() {
		rf.reinstallSnapshot(persister.ReadSnapshot())
		rf.loop()
	}()

	// send heartbeat whenever it is a leader
	// initialize from state persisted before a crash

	return rf
}