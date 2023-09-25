rm -f *.log
go test -run TestInitialElection2A -race

cat raft.log | grep raft-1 > raft-1.log
cat raft.log | grep raft-2 > raft-2.log
cat raft.log | grep raft-0 > raft-0.log
