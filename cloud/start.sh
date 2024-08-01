#!/usr/bin/env bash
cd /vearch/bin/
cur_dir=$(dirname $(readlink -f "$0"))
BasePath=$(
    cd $(dirname $0)
    pwd
)
cd $BasePath

CPUS=`cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us` && [ -n $CPUS ] && [ $CPUS -gt 0 ] && CPUS=`expr $CPUS / 100000` && echo $CPUS && export OMP_NUM_THREADS=$CPUS

function getServiceStatusInfo {
    pidFile=$1
    filterTag=$2
    if [ ! -f "${pidFile}" ]; then
        echo ""
    else
        ps -ef | grep $(cat ${pidFile}) | grep -v grep | grep ${filterTag}
    fi
}

function start {
    stype=$1
    info=$(getServiceStatusInfo "${stype}.pid" "${stype}")
    if [ -z "$info" ]; then
        export LD_LIBRARY_PATH=$cur_dir/lib/:$LD_LIBRARY_PATH
        nohup $BasePath/bin/vearch -conf $BasePath/config.toml $1 >$BasePath/vearch-startup-${stype}.log 2>&1 &
        pid=$!
        echo $pid >$BasePath/${stype}.pid
        echo "[INFO] ${stype} started... pid:${pid}"
    else
        echo "[Error]The ${stype} is running and the ${stype}'s status is :"
        echo "[INFO] status of ${stype} : ${info}"
    fi
    echo "--------------------------------------------------------------------------"
}

if [ -z "$1" ]; then
    echo "start args is empty"
fi

if [ -n "$1" ]; then
    start $1
fi

/usr/sbin/crond

echo "*/1 * * * * cd /vearch && sh restart.sh $1 >> restart.log 2>&1 &" >> /var/spool/cron/root

sleep 9999999d
