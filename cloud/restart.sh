#!/bin/bash
BasePath=$(cd `dirname $0`; pwd)
module=$1
cd $BasePath

export LD_LIBRARY_PATH=/vearch/lib/:/usr/local/nvidia/lib64:$LD_LIBRARY_PATH
CPUS=`cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us` && [ -n $CPUS ] && [ $CPUS -gt 0 ] && CPUS=`expr $CPUS / 100000` && echo $CPUS && export OMP_NUM_THREADS=$CPUS
CheckRestart() {
    process='config'
    log="/vearch/check_process.log"
    data=$(date +"%Y-%m-%d %H:%M:%S")
    for i in $process
        do
        exists=`ps -ef|grep "$i"|grep -v grep|wc -l`
        if [ "$exists" -eq "0" ]; then
            nohup $BasePath/bin/vearch -conf $BasePath/config.toml ${module} >> $BasePath/vearch-startup-${module}.log 2>&1 &
            if [ "$?" -eq "0" ]; then
                echo "${data} : restart sucess" >> $log
            else
                echo "${data} : restart failed" >> $log
        fi       
        pid=$!
        echo $pid > /vearch/${stype}.pid
        echo "[INFO] ${stype} started... pid:${pid}"
        fi
        done
}

CheckRestart
