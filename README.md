# Raft - Distributed Systems Course

Raft is a distributed consensus algorithm that distributes a state machine across a computing cluster. A replicated service achieves fault tolerance by storing complete copies of its state (i.e., data) on multiple replica servers. Replication allows the service to continue operating even if some of its servers experience failures (crashes or a broken or flaky network). The challenge is that failures may cause the replicas to hold differing copies of the data.

Raft organizes client requests into a sequence, called the log, and ensures that all the replica servers see the same log. Each replica executes client requests in log order, applying them to its local copy of the service's state. Since all the live replicas see the same log contents, they all execute the same requests in the same order, and thus continue to have identical service state. If a server fails but later recovers, Raft takes care of bringing its log up to date. Raft will continue to operate as long as at least a majority of the servers are alive and can talk to each other. If there is no such majority, Raft will make no progress, but will pick up where it left off as soon as a majority can communicate again.

My repo is a naive implementation of Raft. It is divided into 4 main parts for testing. 

2A: Leader election 

2B: log implementation

2C: persistence storage

2D: log compaction (snapshots)

To run the tests:
```
go test -run test_name -race -count number_of_times
```
