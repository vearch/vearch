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


# -*- coding: utf-8 -*-
"""VGG16 model for Keras.

# Reference

- [Very Deep Convolutional Networks for Large-Scale Image Recognition](https://arxiv.org/abs/1409.1556)

"""
import cv2
import numpy as np
import tensorflow as tf
from keras.applications.vgg16 import VGG16
from keras.applications.vgg16 import preprocess_input
# import keras.backend.tensorflow_backend as KTF

# config = tf.ConfigProto()
# config.gpu_options.per_process_gpu_memory_fraction = 0.2
# sess = tf.Session(config=config)
# KTF.set_session(sess)


class VGG16Model(object):

    def preprocess_input(self,img):
        return preprocess_input(img)

    def load_model(self):
        input_shape=(224, 224, 3)
        self.model = VGG16(include_top=False, weights='imagenet',
                      input_tensor=None, input_shape=input_shape,
                      pooling="max")
        # weights_path = os.path.join(
        #                     os.path.dirname(
        #                         os.path.dirname(os.path.abspath(__file__))),
        #                     "data",
        #                     _MODEL_WEIGHTS_PATH)
        # print(weights_path)
        # vgg16.load_weights(weights_path)
        # vgg16.load_weights()

    def forward(self,img):
        img = cv2.resize(img, (224, 224))
        img = img[np.newaxis,:]
        img = self.preprocess_input(img)
        result = self.model.predict(img)
        return result

def load_model():
    model = VGG16Model()
    return model

def test():
    model = load_model()
    model.load_model()
    import urllib.request
    def test_url(imageurl):
        resp = urllib.request.urlopen(imageurl).read()
        image = np.asarray(bytearray(resp), dtype="uint8")
        image = cv2.imdecode(image, cv2.IMREAD_COLOR)
        feat = model.forward(image)
        return (feat[0]/np.linalg.norm(feat[0])).tolist()

    print(test_url(url))

if __name__ == "__main__":
    import sys
    test(sys.argv[1])
