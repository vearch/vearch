#!/usr/bin/env bash
BasePath=$(cd `dirname $0`; pwd)
cd $BasePath

function getServiceStatusInfo {
    pidFile=$1
    filterTag=$2
    if [ ! -f "${pidFile}" ]; then
        echo ""
    else
        ps -ef|grep `cat ${pidFile}`|grep -v grep|grep ${filterTag}
    fi
}

function start
{
    stype=$1
    info=$(getServiceStatusInfo "${stype}.pid" "${stype}")
    if [ -z "$info" ]; then
         nohup $BasePath/vearch -conf $BasePath/config.toml $1 > $BasePath/vearch-${stype}-startup.log 2>&1 &
         pid=$!
         echo $pid > $BasePath/${stype}.pid
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

