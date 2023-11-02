rm -f ./log/*.log

go test -run TestSnapshotBasic2D
#go test -run TestFigure8Unreliable2C
#go test -run TestSnapshotInstall2D
#go test -run TestSnapshotInstallUnreliable2D
#go test -run TestSnapshotInstallUnCrash2D
#go test -run 2A
#go test -run 2B
#go test -run 2C
#go test -run 2D


cat ./log/raft.log | grep raft-1 > ./log/raft-1.log
cat ./log/raft.log | grep raft-2 > ./log/raft-2.log
cat ./log/raft.log | grep raft-0 > ./log/raft-0.log
cat ./log/raft.log | grep raft-4 > ./log/raft-4.log
cat ./log/raft.log | grep raft-3 > ./log/raft-3.log

