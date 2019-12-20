#!/bin/bash

path=`pwd`
cd $path/src
pip install -r requirements.txt

run_image_retrieval(){
    python main.py --model_name=image_retrieval
}

run_face_retrieval(){
    python video.py
}

run_text(){
    python main.py --model_name=text
}

modelname=$1
if [ $modelname == "image" ];then
    run_image_retrieval
elif [ $modelname == "video" ];then
    run_face_retrieval
elif [ $modelname == "text" ];then
    run_text
else
    echo "no model"
fi
