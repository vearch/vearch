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

function stop
{
    stype=$1
    echo "  [INFO] Stoping ${stype} :"
    info=$(getServiceStatusInfo "$BasePath/${stype}.pid" "${stype}")
    if [ -z "$info" ]; then
        echo "  [INFO] There is no ${stype}'s pid file！"
    else
        echo "${info}"|awk '{print $2}'|xargs kill -9
        /bin/rm -f ${BasePath}/${stype}.pid
    fi
}
stop $1
#ps -ef|grep vearch|grep -v grep|awk '{print $2}'|xargs kill -9
