# Vearch Compile and Deploy

## Docker Deploy

#### Docker Hub Image Center 
 1. vearch base compile environment image address: https://hub.docker.com/r/vearch/vearch-dev-env/tags
 2. vearch deploy image address: https://hub.docker.com/r/vearch/vearch/tags

#### Use Vearch Image Deploy
 1. If deploying a docker start vearch, master, ps, and router start together
   ```bash
   cp vearch/config/config.toml .
   nohup docker run -p 8817:8817 -p 9001:9001 -v $PWD/config.toml:/vearch/config.toml vearch/vearch:latest all &
   ```
 
 2. If distributed deploy, modify `vearch/config/config.toml` and start separately.
 3. Modify `vearch/config/config.toml`, refer to the step `Local Model`
 4. Start separate image, modify step 1 `all` to `master` and `ps` and `router`, master image must first start

#### Use Base Image Compile And Deploy
 1. take `vearch-dev-env:latest` as an example, and `cd cloud`
 2. `docker run --privileged -i -v $(dirname "$PWD"):/vearch vearch/vearch-dev-env:latest /vearch/cloud/compile/compile.sh`
 3. `cp -r ../build/bin compile/; cp -r ../build/lib compile/; docker build -t vearch/vearch:latest .`
 4. reference **Use vearch image deploy** step 3

#### Use Script Create Base Image And Vearch Image
 1. build compile base environment image 
    1. go to `$vearch/cloud/env` dir
    2. run `docker build -t vearch/vearch-dev-env:latest .` you will get an image named vearch-dev-env
 2. compile vearch
    1. go to `$vearch/cloud` dir
    2. run `docker run --privileged -i -v $(dirname "$PWD"):/vearch vearch/vearch-dev-env:latest /vearch/cloud/compile/compile.sh` you will compile Vearch in $vearch/build/bin, $vearch/build/lib
 3. make vearch image
    1. go to `$vearch/cloud` dir
    2. run `cp -r ../build/bin compile/; cp -r ../build/lib compile/; docker build -t vearch/vearch:latest .` you will get an image named vearch good luck
 4. how to use it 
    1. you can use `docker run -it -v config.toml:/vearch/config.toml vearch all` to start vearch by local model the last param has four types [ps, router, master, all] all means tree type to start
 5. One-click build vearch image
    1. go to `$vearch/cloud` dir
    2. you can run `./run_docker.sh`
