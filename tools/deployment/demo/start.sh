#!/usr/bin/env bash
BasePath=$(cd `dirname $0`; pwd)
cd $BasePath

# mount a local path to cubefs, The cubefs cluster must already exists, this feature is default close
MOUNT_CEBE=0
MOUNT_POINT=XXXXXXX
VOL_NAME=XXXXXXX
OWNER=XXXXXXX
AK=XXXXXXX
SK=XXXXXXX
MASTER_ADDR=XXXXXXX
LOG_DIR=XXXXXXX
MOUNT_SCRIPT_URL=XXXXXXX

function mount {
    if [[ ${MOUNT_CEBE} -eq 0 ]];then
       echo "cubefs feature is close, will start vearch with local disk path"
       return
    fi
    echo "will mount local path ${MOUNT_POINT} to cubefs ..."
    curl ${MOUNT_SCRIPT_URL} |bash -s  ${MOUNT_POINT} ${VOL_NAME} ${OWNER} ${AK} ${SK} ${MASTER_ADDR} ${LOG_DIR}
    # check if the cubefs client works 
    ps -ef | grep cfs-client | grep -v grep
    if [[ ${ret} -ne 0 ]];then
        echo "cubefs mount fail, will start vearch with local disk path"
    fi
}

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
    mount
    start $1 
fi

