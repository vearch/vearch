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
import cv2
import json
import base64
import requests
import urllib.request
import numpy as np

import config
import util
import exceptions

_HEADERS = {"content-type": "application/json"}


class Base(object):
    def pkg_insert(self, ip, data, res):
        bbox = res.pop("boundingbox", None)
        if bbox:
            bbox = list(map(str, bbox))
            data["boundingbox"] = ",".join(bbox)
        label = res.pop("label", None)
        if label:
            data["label"] = label
        # feat = res.pop("feat")
        # feat = feat[0]/np.linalg.norm(feat[0])
        data["feature"] = {"source": data["imageurl"], "feature": res.pop("feat")}
        # print(data)
        response_body = requests.post(ip, headers=_HEADERS, data=json.dumps(data))
        return response_body

    def pre_process(self, data, image):
        self.detection = data.pop("detection", True)
        return image

    def insert(self, ip, data):
        results = []
        # imageurl = data["imageurl"]
        try:
            image = util.read_image(data["imageurl"])
        except Exception as err:
            raise exceptions.ImageError()
        image = self.pre_process(data, image)
        result = self.encode(image)
        for res in result:
            response_body = self.pkg_insert(ip, data, res)
            results.append(response_body.json())
        return results

    def search(self, ip, data):
        min_score = data.pop("score", 0)
        filters = data.pop("filter", None)
        imageurl = data["imageurl"]
        image = util.read_image(imageurl)
        image = self.pre_process(data, image)
        result = self.encode(image)
        feat = result[0]["feat"]
        data = {"query": {"sum": [{"feature": feat, "field": "feature", "min_score": min_score, "max_score": 1.0}],"filter":filters}}
        response_body = requests.post(ip, headers=_HEADERS, data=json.dumps(data))
        return response_body.json()


class Face(Base):

    def __init__(self):
        super(Face, self).__init__()
        model_name = config.face_config["modelname"]
        self.face = util.get_model(model_name)
        self.model = self.face.load_model(config.face_config)

    def encode(self, image):
        return self.model.encode(image)


class ImageSearch(Base):

    def __init__(self):
        super(ImageSearch, self).__init__()
        model_name = config.image_config["modelname"]
        detect_name = config.image_config["detectname"]
        self.extract_model = util.get_model(model_name).load_model()
        self.detect_model = util.get_model(detect_name).load_model() if detect_name else None

    def pre_process(self, data, image):
        self.detection = data.pop("detection", True)
        boundingbox = data.get("boundingbox", None)
        if boundingbox:
            bbox = list(map(int, boundingbox.split(",")))
            image = util.crop(image, bbox)
        return image

    def encode(self, image):
        results = []
        boundingboxes = [[]]
        if self.detection:
            assert self.detect_model is not None, "detect model is not defined"
            boundingboxes = self.detect_model.detect(image)
        for box in boundingboxes:
            if box:
                label, score, bbox = box
                image_crop = util.crop(image, bbox) 
            else:
                label, score, bbox = None, None, None
                image_crop = image
            feat = self.extract_model.forward(image_crop)
            results.append(dict(label=label,
                                feat=util.normlize(feat[0]),
                                boundingbox=bbox
                                )
                           )
        return results


def load_model(model_name):
    if model_name == "face_retrieval":
        util.install_package("tensorflow==1.12.0 mtcnn")
        model = Face()
    elif model_name == "image_retrieval":
        util.install_package("torchvision torch")
        model = ImageSearch()
    else:
        raise exceptions.LoadModelError(f"{model_name} is not existed")

    return model


