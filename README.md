# distributed-storage

## 简介

- 这是一个实验性的分布式存储系统，基于C/S架构。写这个的原因是某一天在一个技术小组内讨论校园网文件分享的功能，因为网上没有合适的轮子，所以萌生了自己造一个的想法。现在的系统基本完善，可以上线使用了。
- 系统原理很简单，数据库由客户端进行操作，服务器负责存储数据。客户端上传文件会对要上传的文件进行分块，每个分块用它的hash值命名，然后上传到服务器，更新全局数据库。客户端下载会查询数据库，选择合适的服务器进行下载，最后将所有分块合并成文件。
- 这个系统的存储是双副本的，能容忍一个节点掉线。
- ~~能够适应校园网是动态IP的问题，服务器启动后会扫描本地存储块并更新到全局数据库中。~~(新版本丢失了此特性，待修复)
- 理论上可以实现一个节点掉线后重新创建副本，下载上传可以实现断电续传。这些特性留待以后看心情实现。
- 关于这个分布式存储为什么选择go语言，因为我觉得go语言最合适。我考虑过python和c，python不能编译成二进制文件，不方便移植，而c编写太复杂，不想折腾。然后想起之前面试时面试官提过go语言，我就查了下，觉得特别合适，而且交叉编译十分方便，于是便边学go边写这个系统。（我学go做的第一个项目）
- 系统的难点也挺多的，比如全局数据库的一致性、系统高可用的实现、上传下载时最佳服务器的选择等等。
- TODO：实现fuse，修复win下服务器掉线导致执行status命令时崩溃的BUG

## 编译

- 先安装依赖
```shell
go get -tags purego modernc.org/ql
go get github.com/remeh/sizedwaitgroup
```

- 下载代码和编译
```shell
cd $GOPATH/src # 切换到GOPATH的src目录
git clone https://github.com/chn-lee-yumi/distributed-storage.git --depth=1 # 下载代码
mv distributed-storage dss # 重命名文件夹名字为dss
cd dss # 进入代码文件夹
go build # 编译项目，生成名为dss的可执行文件
```

- 在任何目录下编译
```shell
go build dss # 编译项目，在当前路径下生成名为dss的可执行文件
```

- 交叉编译：
```shell
# shell
GOARCH=386 GOOS=windows go build dss
```
```shell
# cmd
set GOARCH=386
set GOOS=windows
go build dss
```
- 注：在编译除了386、amd64、arm之外的架构时，会有一个库报错（bigfft），这个时候只要把缺少的文件从go官方库（好像是math/big）里复制到那个库的代码文件夹下，然后对照着库的其它文件，改改复制过去的官方文件就可以了。详细的步骤以后再写。

## 部署

- 建议新建一个文件夹放可执行文件。将server_list.txt的内容改成首节点IP和端口，如下所示。（所有服务器、客户端的这个文件都需要修改，只需要第一次修改即可，后面能连上服务器就会自动更新。）
```text
192.168.1.1:2333
```
- 首节点部署，只要执行以下命令。其中-port参数为服务器的端口，是可选的，默认为2333。以后如果这个节点重启了，按其它节点的命令执行，不再需要执行首节点的命令。首节点命令只在整个集群还没有服务器的时候执行。
```shell
./dss -enable_server -first_server [-port 2333]
```
- 其它节点部署，只要执行以下命令：
```shell
./dss -enable_server [-port 2333]
```
- 客户端直接执行`./dss`运行即可。输入`help`可以查看帮助。
- 如果服务端前面有个路由器做NAT，那么需要配置端口映射，外面的端口号需要跟服务器端口号一致。
- 注意：如果在一台机器上同时运行客户端和服务端，它们不能在同一个文件夹下，需要在不同路径执行，否则可能损坏数据！
