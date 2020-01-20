# Copyright 2019 The Vearch Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# ==============================================================================

import os

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

port = 4101
gpus = '-1'
master_address = 'http://127.0.0.1:8817'
router_address = 'http://127.0.0.1:9001'

face_config = dict(modelname='face_retrieval.face',
                   model_path=os.path.join(root_path, 'model', '20180402-114759'))

image_config = dict(modelname='image_retrieval.image_extract.vgg16',
                    detectname=None)

video = dict(db='video',
             space='video',
             ip='http://127.0.0.1',
             imagepath=os.path.join(root_path, 'images', 'face_retrieval'),
             videopath='rtmp://58.200.131.2:1935/livetv/hunantv')

text = dict(modelname='text.text',
            model_path=os.path.join(root_path, 'model', 'chinese_L-12_H-768_A-12'))
