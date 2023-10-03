rm -f ../log/*.log

cd ../
#go test -run 2A
go test -run TestFailAgree2B

cat ./log/raft.log | grep raft-1 > ./log/raft-1.log
cat ./log/raft.log | grep raft-2 > ./log/raft-2.log
cat ./log/raft.log | grep raft-0 > ./log/raft-0.log
