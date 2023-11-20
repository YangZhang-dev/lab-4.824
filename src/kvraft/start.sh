rm -rf ./log/*
go test -run 3
cd ./sh
./part.sh ../log/kv.log