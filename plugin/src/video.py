# Copyright 2019 The Vearch Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# ==============================================================================
import json
import sys
import time
import os
import requests
import subprocess

import config
import exceptions


# install ffmpeg method
def installFFmpeg():
    p = subprocess.call("bash ../bin/centos_yum_install_ffmpeg.sh", shell=True)
    if p == 0:
        raise exceptions.InstallError("FFmpeg install failed!")
    print("FFmpeg install success!")

def create_DB_space():
    data = {"db": True,
            "method": 1,
            "columns": {
                "imageurl": {
                    "type": "keyword"
                },
                "boundingbox": {
                    "type": "keyword"
                },
                "label": {
                    "type": "keyword"
                }
            },
            "feature": {
                "type": "vector",
                "filed": "imageurl",
                "model_id": "vgg16",
                "dimension": 512
            }
            }
    res = requests.get(f"{config.master_address}/db/{config.video['db']}")
    if res.json()["code"] == 200:
        data["db"] = False
    ip = f"{config.video['ip']}:{config.port}/{config.video['db']}/{config.video['space']}/_create"
    headers = {"content-type":"application/json"}
    res = requests.post(ip, data=json.dumps(data), headers=headers)
    return res.json()

def run():
    main_p = subprocess.Popen(f"python main.py  --model_name=face_retrieval", shell=True)
    process_list.append(main_p)
    # wait 10s for main_p load model
    time.sleep(10)
    res = create_DB_space()

    if res["code"] in [200, 550, 561]:
        print(res)
    else:
        raise exceptions.CreateDBAndSpaceError()

    imgpath = config.video["imagepath"]
    isExists = os.path.exists(imgpath)
    path = config.video["videopath"]
    if not isExists:
        os.makedirs(imgpath)
    ffmpeg_p = subprocess.Popen(
        "ffmpeg -i " + path + " -vf select='eq(pict_type\,I)' -vsync 2 -f image2 " + imgpath + "img-%d.jpg", shell=True)
    process_list.append(ffmpeg_p)
    count = 1
    while True:
        if main_p.poll() is not None:
            raise Exception("main process is killed!")
        if ffmpeg_p.poll() is not None:
            raise Exception("ffmpeg process is killed!")
        imagefile = imgpath + f"/img-{count}.jpg"
        if os.path.exists(imagefile):
            res = requests.post(
                f"{config.video['ip']}:{config.port}/{config.video['db']}/{config.video['space']}/_insert",
                data=json.dumps({"imageurl":imagefile}),
                headers={"content-type":"application/json"}
            )
        count += 1
        time.sleep(1)

def clear(process_list):
    for p in process_list:
        if p.poll() is None:
            p.terminate()
    sys.exit(1)

if __name__ == "__main__":
    # judge if ffmpeg has been installed
    p = subprocess.run("ffmpeg -version", shell=True)
    if p.returncode != 0:
        installFFmpeg()
    process_list = []
    try:
        run()
    except BaseException as err:
        print(err)
        clear(process_list)



