#Codis安装部署

Codis的官网推荐的安装部署方式是通过git获取master分支的代码然后进行编译部署，但是在实际的生产环境中很可能部署机是无法连接到外网到，
也就不能使用git。本文将介绍如果将所需要到软件包事先下载好，然后通过非root用户进行安装部署。

前期准备
-------------

####所需到安装包


| Name | Version | 
|:----: |:----: |
|go1.6.linux-amd64.tar.gz|1.6|
|godep_linux_amd64|V74|
|codis-master.zip|－|

####创建一个非root用户，例如新建用户ocdp

安装 Go
-----------------------

1.使用ocdp用户登录，并创建路径`/home/ocdp/Applications`

2.解压go1.6.linux-amd64.tar.gz到`/home/ocdp/Applications`

3.将如下内容加入到`/home/ocdp/.bash_profile`中

```bash
export GOROOT=/home/ocdp/Applications/go

export PATH=$PATH:$GOROOT/bin

export GOPATH=/home/ocdp/Applications/workspace
```
4.并执行`source /home/ocdp/.bash_profile`使之生效

5.将godep_linux_amd64复制到`/home/ocdp/Applications/go/bin`并重命名为godep，修改权限为755


编译Codis
-----------------------
1.执行
```bash
mkdir -p /home/ocdp/Applications/workspace/src/github.com/CodisLabs
```

2.将codis-master.zip解压到`/home/ocdp/Applications/workspace/src/github.com/CodisLabs`并重命名为codis

3.进入`/home/ocdp/Applications/workspace/src/github.com/CodisLabs/codis` 执行`make`

4.执行成功之后会在`/home/ocdp/Applications/workspace/src/github.com/CodisLabs/codis/bin`下面
生成codis-config、codis-proxy、codis-server三个可执行文件


部署Codis(可选)
-----------------------
如果要将codis应用和源码分离，可以参考此部分
```bash
mkdir -p /home/ocdp/Applications/codis/{log,conf,data,bin}
cd /home/ocdp/Applications/workspace/src/github.com/CodisLabs/codis
cp -rf bin/* /home/ocdp/Applications/codis/bin
cp config.ini /home/ocdp/Applications/codis/conf
cp -rf /home/ocdp/Applications/workspace/src/github.com/CodisLabs/codis/bin/* /home/ocdp/Applications/codis/bin
cd /home/ocdp/Applications/workspace/src/github.com/CodisLabs/codis/extern/redis-2.8.21/src
cp redis-cli /home/ocdp/Applications/codis/bin/
```

修改相关配置
-----------------------
- 修改config.ini中zk的ip和端口号，dashboard_addr的ip和端口号
- 修改codis服务的配置，默认可以使用系统提供的，例如`cp /home/ocdp/Applications/workspace/src/github.com/CodisLabs/codis/extern/redis-test/conf/6379.conf /home/ocdp/Applications/codis/conf`

启动相关服务
-----------------------
进入`/home/ocdp/Applications/codis`路径

1.启动dashboard
```bash
nohup bin/codis-config dashboard > /dev/null &
```
2.启动codes-server
```bash
nohup bin/codis-server ./conf/6379.conf > /dev/null &
```
3.添加group和master(此步骤也可通过dashboard完成)
```bash
bin/codis-config server add 1 hostname:6379 master
```
4.设置分片(此步骤也可通过dashboard完成)
```bash
bin/codis-config slot init
bin/codis-config slot range-set 0 1023 1 online
```
5.启动proxy
```bash
nohup bin/codis-proxy -c config.ini -L ./log/proxy.log  --cpu=1 --addr="hostname:19000" --http-addr="hostname:11000" > /dev/null &
```
总结
-----------------------
至此Codis部署安装完成，正常的生产环境是要添加多个group，master和slave的，都是可以通过dashboard完成。




