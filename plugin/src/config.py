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

import os
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

port = 4101
gpu = "-1"
master_address = "http://127.0.0.1:8817"
router_address = "http://127.0.0.1:9001"

face_config = dict(is_detect=True,
                   image_size=160,
                   modelname="face_retrieval.face",
                   model_path=os.path.join(root_path, "model", "20180402-114759")
                   )

image_config = dict(modelname="image_retrieval.image_extract.vgg16",
                    detectname="image_retrieval.image_detect.yolo3")

video = dict(db="video",
             space="video",
             ip="http://127.0.0.1",
             imagepath=os.path.join(root_path, "images","face_retrieval"),
             videopath="******")

def test():
    print("face_config:\n", face_config)


if __name__ == "__main__":
    test()
