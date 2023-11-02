source ../../venv/bin/activate
rm -rf ./log/*
#pipenv run python3.8 dstest.py  TestSnapshotBasic2D TestSnapshotInstall2D TestSnapshotInstallUnreliable2D TestSnapshotInstallCrash2D TestSnapshotInstallUnCrash2D   -n 200 -p 8 -o ./log/
pipenv run python3.8 dstest.py  2A  -n 8 -p 8 -o ./log/
#pipenv run python3.8 dstest.py  2A 2B  TestFigure8Unreliable2C   TestUnreliableChurn2C   -n 200 -p 8 -o ./log/
#pipenv run python3.8 dstest.py   TestBasicAgree2B TestRPCBytes2B TestFailAgree2B TestFailNoAgree2B TestConcurrentStarts2B TestRejoin2B TestBackup2B TestCount2B -n 200 -p 8 -o ./log/
#pipenv run python3.8 dstest.py  TestInitialElection2A TestReElection2A TestManyElections2A \
#TestBasicAgree2B TestRPCBytes2B TestFailAgree2B TestFailNoAgree2B TestConcurrentStarts2B TestRejoin2B TestBackup2B TestCount2B\
#TestPersist12C TestPersist22C TestPersist32C TestFigure82C TestUnreliableAgree2C TestFigure8Unreliable2C TestReliableChurn2C TestUnreliableChurn2C\
#TestSnapshotBasic2D TestSnapshotInstall2D TestSnapshotInstallUnreliable2D TestSnapshotInstallCrash2D TestSnapshotInstallUnCrash2D -n 200 -p 7 -o ./log/
