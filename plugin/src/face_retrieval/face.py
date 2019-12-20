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
import re
import cv2
import subprocess
import numpy as np
import tensorflow as tf
from mtcnn.mtcnn import MTCNN


root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
default_model_path = os.path.join(root_path, "model", "20180402-114759")
MODEL_URL = 'https://drive.google.com/uc?id=1EXPBSXwTaqrSC0OhUdXNmKSh9qJUQ55-&export=download'


class FaceRecognition(object):
    def __init__(self, is_detect=True, image_size=160, model_path=default_model_path, **kwargs):
        self.image_size = image_size
        self.encoder = FaceEncoder(model_path)
        self.mtcnn_detect = None
        if is_detect:
            self.mtcnn_detect = MTCNN()

    def detect(self, image):
        faces_bboxes = [bbox["box"] for bbox in self.mtcnn_detect.detect_faces(image)]
        if len(faces_bboxes) == 0:
            return image
        image_h, image_w = image.shape[:2]
        x_min, y_min, w, h = faces_bboxes[0]
        x_min = int(max(x_min, 0))
        x_max = int(min(x_min+w, image_w))
        y_min = int(max(y_min, 0))
        y_max = int(min(y_min + h, image_h))
        image = image[y_min: y_max, x_min: x_max, :]
        return image

    def encode(self, image):
        if self.mtcnn_detect:
            image = self.detect(image)

        image_resize = cv2.resize(image, (self.image_size, self.image_size))
        embedding = self.encoder.generate_embedding(image_resize)
        feat = embedding.tolist()
        return feat


class FaceEncoder(object):
    def __init__(self, model_path):
        if not os.path.exists(model_path):
            model_dir = os.path.dirname(model_path)
            if not os.path.exists(model_dir):
                os.makedirs(model_dir)
            print(f"model not exists, begin download model save in {model_dir}!")
            subprocess.run(
                f"wget -P {model_dir} {MODEL_URL} && cd {model_dir} && unzip 20180402-114759.zip",
                shell=True)

        config = tf.ConfigProto(log_device_placement=False)
        config.gpu_options.allow_growth = True
        self.sess = tf.Session(config=config)
        with self.sess.as_default():
            self.load_model(model_path)
        self.images_placeholder = tf.get_default_graph().get_tensor_by_name("input:0")
        self.embeddings = tf.get_default_graph().get_tensor_by_name("embeddings:0")
        self.phase_train_placeholder = tf.get_default_graph().get_tensor_by_name("phase_train:0")

    def prewhiten(self, x):
        mean = np.mean(x)
        std = np.std(x)
        std_adj = np.maximum(std, 1.0/np.sqrt(x.size))
        y = np.multiply(np.subtract(x, mean), 1/std_adj)
        return y

    def load_model(self, model, input_map=None):
        # Check if the model is a model directory (containing a metagraph and a checkpoint file)
        #  or if it is a protobuf file with a frozen graph
        model_exp = os.path.expanduser(model)
        if (os.path.isfile(model_exp)):
            print('Model filename: %s' % model_exp)
            with gfile.FastGFile(model_exp,'rb') as f:
                graph_def = tf.GraphDef()
                graph_def.ParseFromString(f.read())
                tf.import_graph_def(graph_def, input_map=input_map, name='')
        else:
            print('Model directory: %s' % model_exp)
            meta_file, ckpt_file = self.get_model_filenames(model_exp)

            print('Metagraph file: %s' % meta_file)
            print('Checkpoint file: %s' % ckpt_file)

            saver = tf.train.import_meta_graph(os.path.join(model_exp, meta_file), input_map=input_map)
            saver.restore(tf.get_default_session(), os.path.join(model_exp, ckpt_file))

    def get_model_filenames(self, model_dir):
        files = os.listdir(model_dir)
        meta_files = [s for s in files if s.endswith('.meta')]
        if len(meta_files)==0:
            raise ValueError('No meta file found in the model directory (%s)' % model_dir)
        elif len(meta_files)>1:
            raise ValueError('There should not be more than one meta file in the model directory (%s)' % model_dir)
        meta_file = meta_files[0]
        ckpt = tf.train.get_checkpoint_state(model_dir)
        if ckpt and ckpt.model_checkpoint_path:
            ckpt_file = os.path.basename(ckpt.model_checkpoint_path)
            return meta_file, ckpt_file

        meta_files = [s for s in files if '.ckpt' in s]
        max_step = -1
        for f in files:
            step_str = re.match(r'(^model-[\w\- ]+.ckpt-(\d+))', f)
            if step_str is not None and len(step_str.groups())>=2:
                step = int(step_str.groups()[1])
                if step > max_step:
                    max_step = step
                    ckpt_file = step_str.groups()[0]
        return meta_file, ckpt_file

    def generate_embedding(self, face):
        prewhiten_face = self.prewhiten(face)
        # Run forward pass to calculate embeddings
        feed_dict = {self.images_placeholder: [prewhiten_face], self.phase_train_placeholder: False}
        return self.sess.run(self.embeddings, feed_dict=feed_dict)[0]


def load_model(config=None):
    model = FaceRecognition() if config is None else FaceRecognition(**config)
    return model


if __name__ == "__main__":
    pass
