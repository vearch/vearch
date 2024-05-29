# Vearch Compile and Deploy

## Docker Deploy

#### Docker Hub Image Center 
 1. vearch base compile environment image address: https://hub.docker.com/r/vearch/vearch-dev-env/tags
 2. vearch deploy image address: https://hub.docker.com/r/vearch/vearch/tags

#### Use Vearch Image Deploy
 1. If deploy a docker start vearch, master, ps, router start together
   ```
   cp vearch/config/config.toml .
   nohup docker run -p 8817:8817 -p 9001:9001 -v $PWD/config.toml:/vearch/config.toml vearch/vearch:latest all &
   ```
 
 2. If distributed deploy, modify vearch/config/config.toml and start separately.
 3. Modify vearch/config/config.toml, refer the step 'Local Model'
 4. Start separately image, modify step i 'all' to 'master' and 'ps' and 'router', master image must first start
#### Use Base Image Compile And Deploy
 1. take vearch-dev-env:latest as an example
 2. docker pull vearch/vearch-dev-env:latest
 3. sh vearch/cloud/complile.sh
 4. sh build.sh
 5. reference "Use vearch image deploy" step 3

#### Use Script Create Base Image And Vearch Image
 1. build compile base environment image 
    1. go to $vearch/cloud/env dir
    2. run `docker build -t vearch/vearch-dev-env:latest .` you will got a image named vearch-dev-env
 2. compile vearch
    1. go to $vearch/cloud dir
    2. run ./compile.sh you will compile Vearch in $vearch/build/bin , $vearch/build/lib
 3. make vearch image
    1. go to $vearch/cloud dir
    2. run ./build.sh you will got a image named vearch good luck
 4. how to use it 
    1. you can use docker run -it -v config.toml:/vearch/config.toml vearch all to start vearch by local model the last param has four type[ps, router ,master, all] all means tree type to start
 5. One-click build vearch image
    1. go to $vearch/cloud dir
    2. you can run ./run_docker.sh
