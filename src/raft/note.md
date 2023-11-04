# all
- 如果收到的响应或请求的term比自己的大，无论什么情况，都要设置新的term，然后把自己变为follower
# election

- 如果有一个更大的term发送投票请求，首先要将votefor设为null，更新term

- 何时更新定时器
  - 你从当前的领导者那里得到一个AppendEntries RPC（即，如果AppendEntries参数中的任期已经过时，你不应该重启你的计时器）
  - 你正在开始一个选举
  - 你授予另一个对等体一个投票。

**不要把超时时间设置的太短，后期无法维护!!!**
## entry

leader 发送EV（1）请求，如果没有大多数机器收到（2/5），并且这时候发生了选举，分两种情况：
1. 如果leader继续当选，1会被复制成功
2. 度过是其余三个当选，那么1会被丢弃


2C

不可靠网络+网络分区（crash）=> 超长歧义链（Term频繁变化）

A commit 2 , B,C apply

C 网络分区,作为leader的同时



