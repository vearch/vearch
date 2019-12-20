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
import cv2
import base64
import importlib
import urllib.request
import numpy as np
import subprocess


def get_model(model_path):
    """get model by model_path
    Args:
        model_path: the path of model by user define
    Returns:
        the model
    Raises:
        ModuleNotFoundError model not exist
    """
    try:
        model = importlib.import_module(f'{model_path}')
    except ModuleNotFoundError:
        raise ModuleNotFoundError(f'{model_path} is not existed')
    return model


def read_image(imageurl):
    if '.' in imageurl:
        if imageurl.startswith('http'):
            with urllib.request.urlopen(imageurl) as f:
                resp = f.read()
        elif os.path.exists(imageurl):
            with open(imageurl, 'rb') as f:
                resp = f.read()
        else:
            raise Exception()
    else:
        resp = base64.b64decode(imageurl)
    image = np.asarray(bytearray(resp), dtype='uint8')
    image = cv2.imdecode(image, cv2.IMREAD_COLOR)
    return image


def crop(image, bbox):
    if not bbox:
        return image
    x_min, y_min, x_max, y_max = map(int, bbox)
    img_crop = image[y_min:y_max, x_min:x_max]
    return img_crop


def normlize(feat):
    feat = feat/np.linalg.norm(feat)
    return feat.tolist()


def install_package(name):
    subprocess.run(f'pip install -i https://pypi.tuna.tsinghua.edu.cn/simple {name}', shell=True)


if __name__ == '__main__':
    pass
