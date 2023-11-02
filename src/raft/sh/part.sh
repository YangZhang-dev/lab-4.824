v=$1

s=${v##*/}
name=${s%.*}
pname=`date '+%Y%m%d%H%M%S'-$name`
mkdir ./log/$pname

cp $1 ./log/$pname/$name.log
cp $1 ./log/$pname/raft.log

cat $1 | grep raft-1 > ./log/$pname/raft-1.log
cat $1 | grep raft-2 > ./log/$pname/raft-2.log
cat $1 | grep raft-0 > ./log/$pname/raft-0.log
cat $1 | grep raft-3 > ./log/$pname/raft-3.log
cat $1 | grep raft-4 > ./log/$pname/raft-4.log
cat $1 | grep raft-5 > ./log/$pname/raft-5.log
cat $1 | grep raft-6 > ./log/$pname/raft-6.log

sed '/^raft-[0-9]/d' -i ./log/$pname/raft.log
