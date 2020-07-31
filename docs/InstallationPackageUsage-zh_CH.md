# Vearch安装包使用方法

## 一. 使用过程

**1. 下载地址**

适用于centos7。

百度网盘: https://pan.baidu.com/s/1LyFNtRuUSrH9TmMUY91oTw  提取码: 6u4b

谷歌云盘: https://drive.google.com/drive/folders/1w8KzXdj612rWOxIbZduo5gxNoBAzba7I?usp=sharing

**2. 安装**

​		`sudo sh vearch-3.1.0.sh`

**3. 配置**

| 脚本          | 路径          | 用途               |
| :------------ | :------------ | :----------------- |
| *config.toml* | /etc/vearch.d | vearch启动配置文件 |
| *start.sh*    | /etc/vearch.d | vearch启动脚本     |

*config.toml*配置说明:

（1）数据磁盘存储路径:

​          `data = [ "/export/vearch/datas/",]`  

（2）masters ip 和port:

​		 `address = "127.0.0.1"`   

​         `api_port = 443`   

（3）router port:

​		`port = 80`

**4. 运行**

运行前*start.sh*配置说明:

`SERVER_TYPE=`  `all`. `master`. `router`或`ps`。`all`表示同时启动三个。

执行 : `systemctl start vearch`

**5. 查看运行状态**
`systemctl status vearch`

**6. 停止**
`systemctl kill vearch`



**其他说明:**

使用该方法安装的vearch，目前不支持GPU模型，vearch其他模型均支持。若使用vearch GPU模型请下载源码编译。



## 二. 部署实例

**1. 目标**

使用6台机器，6台机器IP分别为172.0.0.1. 172.0.0.2，172.0.0.3，172.0.0.4. 172.0.0.5和172.0.0.6，6台机器分别部署master1，master2，router1，router2，ps1，ps2。

**2. 下载与安装**

在6台机器上分别执行上述过程   1    和    2 。

**3. 配置**

（1）*config.toml*配置

6台电脑的*config.toml*文件相同。

[global]

`data = ["/home/export/vearch/datas/",]`

[[masters]]

因为两个master，故*config.toml*文件中两份`[[masters]]`，`[[masters]]`下修改内容如下：

 `name = "master1"`    和     `name = "master2"`

`address = "127.0.0.1"`     和     `address = "127.0.0.2"`

[router]

`[router]`只需一份， router port修改为: `port = 88`

（2）*start.sh*配置

`SERVER_TYPE`: 服务器的角色。172.0.0.1~172.0.0.6分别为：`SERVER_TYPE=master`， `SERVER_TYPE=master`, `SERVER_TYPE=router`，`SERVER_TYPE=router`，`SERVER_TYPE=ps`，`SERVER_TYPE=ps`。

**4. 运行**

先启动master，在172.0.0.1和172.0.0.2分别执行`systemctl start vearch`。

然后在172.0.0.3~172.0.0.6分别执行`systemctl start vearch`。

**5. 查看运行状态与终止**

`systemctl kill vearch`      和     `systemctl kill vearch`