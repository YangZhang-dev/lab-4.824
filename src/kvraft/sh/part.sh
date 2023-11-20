v=$1

s=${v##*/}
name=${s%.*}
pname=`date '+%Y%m%d%H%M%S'-$name`
mkdir ./log/$pname

cp $1 ./log/$pname/$name
cp $1 ./log/$pname/kv

cat $1 | grep clerk > ./log/$pname/clerk
cat $1 | grep clerk-0 > ./log/$pname/clerk-0
cat $1 | grep clerk-1 > ./log/$pname/clerk-1
cat $1 | grep clerk-2 > ./log/$pname/clerk-2
cat $1 | grep clerk-3 > ./log/$pname/clerk-3
cat $1 | grep clerk-4 > ./log/$pname/clerk-4
cat $1 | grep clerk-5 > ./log/$pname/clerk-5
cat $1 | grep clerk-6 > ./log/$pname/clerk-6
cat $1 | grep clerk-7 > ./log/$pname/clerk-7
cat $1 | grep server-0 > ./log/$pname/server-0
cat $1 | grep server-1 > ./log/$pname/server-1
cat $1 | grep server-2 > ./log/$pname/server-2
cat $1 | grep server-3 > ./log/$pname/server-3
cat $1 | grep server-4 > ./log/$pname/server-4
cat $1 | grep server-5 > ./log/$pname/server-5
cat $1 | grep raft-0 > ./log/$pname/raft-0
cat $1 | grep raft-1 > ./log/$pname/raft-1
cat $1 | grep raft-2 > ./log/$pname/raft-2
cat $1 | grep raft-3 > ./log/$pname/raft-3
cat $1 | grep raft-4 > ./log/$pname/raft-4
cat $1 | grep raft-5 > ./log/$pname/raft-5
cat $1 | grep raft- > ./log/$pname/raft
cat $1 | grep server- > ./log/$pname/server

sed '/^clerk-/d' -i ./log/$pname/kv
sed '/^server-[0-9]/d' -i ./log/$pname/kv
