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

import cv2
import numpy as np
from PIL import Image
import torch
from torchvision import transforms
import torchvision.models as models

import torch.nn.functional as F

class BaseModel(object):

    def __init__(self):
        self.image_size = 224
        self.dimision = 512

    def load_model(self):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = models.vgg16(pretrained=True, init_weights=False).to(self.device)
        self.model = self.model.eval()
        self.PIXEL_MEANS = torch.tensor((0.485, 0.456, 0.406)).to(self.device)
        self.PIXEL_STDS = torch.tensor((0.229, 0.224, 0.225)).to(self.device)
        self.num = torch.tensor(255.0).to(self.device)

    def preprocess_input(self, image):
        image = cv2.resize(image, (self.image_size, self.image_size))
        # gpu version
        image_tensor = torch.from_numpy(image.copy()).to(self.device).float()
        image_tensor /= self.num
        image_tensor -= self.PIXEL_MEANS
        image_tensor /= self.PIXEL_STDS
        image_tensor = image_tensor.permute(2, 0 ,1)
        return image_tensor

    def forward(self, x):
        # x = torch.stack(x)
        # x = x.to(self.device)
        x = self.preprocess_input(x).unsqueeze(0)
        x = self.model.features(x)
        x = F.max_pool2d(x, kernel_size=(7, 7))
        x = x.view(x.size(0),-1)
        # print(x.shape)
        # x = torch.squeeze(x,-1)
        # x = torch.squeeze(x,-1)
        return self.torch2list(x)

    def torch2list(self, torch_data):
        return torch_data.cpu().detach().numpy().tolist()

def load_model():
    return BaseModel()

def test():
    model = load_model()
    model.load_model()
    import urllib.request
    def test_url(imageurl):
        resp = urllib.request.urlopen(imageurl).read()
        image = np.asarray(bytearray(resp), dtype="uint8")
        image = cv2.imdecode(image, cv2.IMREAD_COLOR)
        feat = model.forward(image)
        return feat[0]/np.linalg.norm(feat[0])

    print(test_url("http://img30.360buyimg.com/da/jfs/t14458/111/1073427178/210435/20d7f66/5a436349Ncf9bea13.jpg"))
    # from PIL import Image
    # image1 = cv2.imread("../../images/test/COCO_val2014_000000123599.jpg")
    # # image2 = cv2.imread("../../images/test/COCO_val2014_000000123599.jpg")
    # print(model.forward(image1[:,:,::-1]))
    # import time
    # start = time.time()
    # tensor1 = model.preprocess_input(image1)
    # tensor2 = model.preprocess_input2(image1)
    # print(time.time()-start)
    # data = [tensor1,tensor2]
    # data = [tensor1]
    # data = torch.stack([tensor1,tensor2])
    # print(data.shape)
    # array1,array2 = model.forward(data)
    # import keras_vgg16 as kv
    # keras_model = kv.model
    # image2 = image1[:,:,::-1]
    # image2 = cv2.resize(image2, (224, 224))
    # image2 = image2[np.newaxis,:]
    # image2 = keras_model.preprocess_input(image2)
    # array2 = keras_model.predict(image2)[0]
    # image2 = image2.transpose(0,3,1,2)
    # array1 = model.forward(model.preprocess_input(image2))[0]


    # print(np.array(array1).shape, np.array(array2).shape)
    # print(np.dot(array1, array2)/(np.linalg.norm(array1) * np.linalg.norm(array2)))


if __name__ == "__main__":
    test()
