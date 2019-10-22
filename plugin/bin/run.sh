#!/bin/bash

path=`pwd`
cd $path/src
pip3 install -r requirements.txt

run_image_retrieval(){
    python3 main.py --model_name=image_retrieval
}

run_face_retrieval(){
    python3 video.py
}

modelname=$1
if [ $modelname == "image" ];then
    run_image_retrieval
elif [ $modelname == "video" ];then
    run_face_retrieval
else
    echo "no model"
fi
