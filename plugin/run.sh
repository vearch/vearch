#!/bin/bash

path=`pwd`

# install pytorch and torchvision
pip install torchvision
pip install torch

# download weight
cd $path
cd model/image_detect
if [ ! -f yolov3.weights ];then
    sh weight.sh
fi

# install package
isexist(){
    pymod=$1
    if python -c "import $pymod" > /dev/null 2>&1
    then
        echo "found ${pymod}"
    else
        echo "not found ${pymod},begin install $2"
        pip install   $2
    fi
}
error_exit(){
    echo ""
    echo "$1"
    exit 1
}
isexist "requests" "requests"  2>/dev/null || error_exit  "install requests failed"
isexist "tornado" "tornado==6.0.2"  2>/dev/null || error_exit  "install tornado==6.0.2 failed"
isexist "shortuuid" "shortuuid"   2>/dev/null || error_exit  "install shortuuid failed"
isexist "cv2" "opencv-python"  2>/dev/null || error_exit "install opencv-python failed"

echo "Begin torndao service!!!"
cd $path
python main.py --port=4101 --gpu=0
