# Vearch installation package usage

## Single deployment

**1. Download address**

For centos7.

Baidu cloud:  https://pan.baidu.com/s/1LyFNtRuUSrH9TmMUY91oTw Extract the code: 6u4b

Google cloud disk: https://drive.google.com/drive/folders/1w8KzXdj612rWOxIbZduo5gxNoBAzba7I?usp=sharing

**2. Installation**

​        `sudo sh vearch-3.1.0.sh`

**3. Configuration**

| **Script**    | Path          | Use                       |
| :------------ | :------------ | :------------------------ |
| *config.toml* | /etc/vearch.d | vearch configuration file |
| *start.sh*    | /etc/vearch.d | vearch startup scrip      |

*config.toml* configuration instructions:

（1）Data disk storage path:

​        `data = [ "/export/vearch/datas/",]`  

（2）masters ip 和port:

​        `address = "127.0.0.1"`   

​        `api_port = 443`   

（3）router port:

​        `port = 80`

**4. Run**

*start.sh* configuration instructions before running:

`SERVER_TYPE=all` .   `all` represents running *master*, *router*, and *ps* on the same machine at the same time.

Perform：`systemctl start vearch`

**5. Views health status**
`systemctl status vearch`

**6. Stop**
`systemctl kill vearch`



**Other:**

Vearch installed using this approach does not currently support the GPU model. Vearch's other models are supported. To use the Vearch GPU model, download the source code for compilation.



## Distributed deployment

**1. Target**

Six machines are used. The IP of the six machines is: 172.0.0.1, 172.0.0.2, 172.0.0.3, 172.0.0.4, 172.0.0.5 and 172.0.0.6.  6 machines are deployed separately: master1, master2, router1, router2, ps1, ps2.

**2. Download and Install**

Perform steps 1 and 2 above on each of the six machines.

**3. Configuration**

（1）*config.toml*  configuration

The *config.toml* file is the same for 6 computers.

[global]

`data = ["/home/export/vearch/datas/",]`

[[masters]]

Because there are two masters. Therefore, there are two `[[masters]]` in *config.toml* file.  The following modifications are required under `[[Masters]]`:

`name = "master1"`    and     `name = "master2"`

`address = "127.0.0.1"`     and     `address = "127.0.0.2"`

[router]

`[router]` only needs one copy. Router port is changed to: `port = 88`

（2）*start.sh*  configuration

`SERVER_TYPE`：Role of the server。172.0.0.1~172.0.0.6 is modified to: `SERVER_TYPE=master`, `SERVER_TYPE=master`,  `SERVER_TYPE=router`, `SERVER_TYPE=router`, `SERVER_TYPE=ps`,  `SERVER_TYPE=ps`.

**4. Run**

Start the master first.  `systemctl start vearch` was executed on the 172.0.0.1 and 172.0.0.2 machines, respectively.

Then execute `systemctl start vearch` in 172.0.0.3~172.0.0.6 machines, respectively.

**5. View status and Termination**

`systemctl status vearch`     and     `systemctl kill vearch`