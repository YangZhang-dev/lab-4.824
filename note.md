# env

``` bash
go build -race -gcflags="all=-N -l"  -buildmode=plugin ../mrapps/wc.go
zzys@ubuntu:~/6.824/src/main$ 
```
防止Goland Debug时
```
plugin.Open("/home/zzys/6.824/src/main/wc"): plugin was built with a different version of package internal/abi
```
错误
