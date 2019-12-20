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

import config
import util
import exceptions


class Base(object):
    pass


class Face(Base):

    def __init__(self):
        super(Face, self).__init__()
        model_name = config.face_config['modelname']
        self.face = util.get_model(model_name)
        self.model = self.face.load_model(config.face_config)

    def pre_process(self, image, boundingbox):
        if boundingbox:
            bbox = list(map(int, boundingbox.split(',')))
            image = util.crop(image, bbox)
        return image

    def encode(self, url):
        try:
            image = util.read_image(url)
        except Exception as err:
            raise exceptions.ImageError(url)
        # image = self.pre_process(image)
        res = self.model.encode(image)
        return res


class ImageSearch(Base):

    def __init__(self):
        super(ImageSearch, self).__init__()
        model_name = config.image_config['modelname']
        detect_name = config.image_config['detectname']
        self.extract_model = util.get_model(model_name).load_model()
        self.detect_model = util.get_model(detect_name).load_model() if detect_name else None

    def pre_process(self, image):
        if self.detect_model:
            bbox = self.detect_model.detect(image)
            image = util.crop(image, bbox)
        return image

    def encode(self, url):
        try:
            image = util.read_image(url)
        except Exception as err:
            raise exceptions.ImageError(f'read {url} failed!')
        image = self.pre_process(image)
        feat = self.extract_model.forward(image)
        assert len(feat) > 0, 'No detect object.'
        return feat[0]


class Text(Base):

    def __init__(self):
        super(Text, self).__init__()
        model_name = config.text['modelname']
        text = util.get_model(model_name)
        self.model = text.load_model(config.text)

    def encode(self, text):
        return self.model.encode([text])[0]


def load_model(model_name):
    if model_name == 'face_retrieval':
        model = Face()
    elif model_name == 'image_retrieval':
        model = ImageSearch()
    elif model_name == 'text':
        model = Text()
    elif model_name == 'audio':
        raise NotImplementedError()
    else:
        raise exceptions.LoadModelError(f'{model_name} is not existed')

    return model


