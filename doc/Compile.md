## Compile BaudEngine

> if you only need use it , you can skip this page , to down binary package  

##### Get Source 

````
git clone xxxxxxxxxxxxxxxxxx/baudengine.git
go get xxxxxxxxxxxxxxxxxx/caprice.git

or 
down zip in git pages

````

### Environment requirement

* system linux (when you use vector suggest centos 7+) , macos  not support windows
* golang 1.11.2+
* if you need vectory  must install faiss and some env


#### Compile

* use vector
````$bash
 export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/vdb/src/github.com/tiglabs/baudengine/engine/gammacb/lib/lib/;go build -a --tags=vector -o /home/vdb/bin/baudengine
````

* only full text

````$bash
  go build -a -o /home/vdb/bin/baudengine
````

when over you got `baudengine` file good job, you have got it 