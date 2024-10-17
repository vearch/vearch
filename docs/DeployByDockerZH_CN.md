# Vearch编译和部署

## Docker部署

#### Docker Hub Image Center 
 1. vearch基础编译环境镜像地址: https://hub.docker.com/r/vearch/vearch-dev-env/tags
 2. vearch部署镜像地址: https://hub.docker.com/r/vearch/vearch/tags

#### 使用Vearch镜像部署
 1. 如果部署时用docker同时启动master, ps, router
   ```
   cp vearch/config/config.toml .
   nohup docker run -p 8817:8817 -p 9001:9001 -v $PWD/config.toml:/vearch/config.toml vearch/vearch:latest all &
   ```
 
 2. 如果使用分布式部署，修改vearch/config/config.toml，分别启动.
 3. 参考步骤 '单机模式' 修改vearch/config/config.toml.
 4. 分别启动镜像，将角色从all修改为master，ps，router，必须首先启动master.

#### 使用基础镜像编译和部署
 1. 以vearch-dev-env:latest为例
 2. `docker build -f cloud/Dockerfile -t vearch/vearch:latest .`
 3. 参考“使用Vearch镜像部署”步骤3

#### 使用脚本创建基础镜像和vearch镜像
 1. 构建编译基础环境镜像
    1. 进入$vearch/cloud/env目录
    2. 执行`docker build -t vearch/vearch-dev-env:latest .`，你将得到一个名为vearch-dev-env的镜像
 2. 制作vearch镜像
    执行`docker build -f cloud/Dockerfile -t vearch/vearch:latest .`， 你将得到一个vearch的镜像
 3. 使用方法 
    1. 执行 `docker run -it -v config.toml:/vearch/config.toml vearch all`  all表示master、router、ps同时启动，也可以使用master\router\ps分开启动
 4. 一键构建vearch镜像
    1. 进入$vearch/cloud目录
    2. 执行./run_docker.sh
