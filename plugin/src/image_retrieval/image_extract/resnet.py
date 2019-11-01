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
        self.dimision = 2048
        self.load_model()

    def load_model(self):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = models.resnet50(pretrained=True).to(self.device)
        self.model = self.model.eval()
        self.PIXEL_MEANS = torch.tensor((0.485, 0.456, 0.406)).to(self.device)
        self.PIXEL_STDS = torch.tensor((0.229, 0.224, 0.225)).to(self.device)
        self.num = torch.tensor(255.0).to(self.device)
        # self.model.cuda()

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
        x = self.model.conv1(x)
        x = self.model.bn1(x)
        x = self.model.relu(x)
        x = self.model.maxpool(x)

        x = self.model.layer1(x)
        x = self.model.layer2(x)
        x = self.model.layer3(x)
        x = self.model.layer4(x)
        x = F.avg_pool2d(x, kernel_size=x.size()[2:])
        x = torch.squeeze(x,-1)
        x = torch.squeeze(x,-1)
        return self.torch2list(x)

    def torch2list(self, torch_data):
        return torch_data.cpu().detach().numpy().tolist()

def load_model():
    return BaseModel()


def test(url):
    model = load_model()
    # model.load_model()
    import urllib.request
    def test_url(imageurl):
        resp = urllib.request.urlopen(imageurl).read()
        image = np.asarray(bytearray(resp), dtype="uint8")
        image = cv2.imdecode(image, cv2.IMREAD_COLOR)
        feat = model.forward(image)
        return (feat[0]/np.linalg.norm(feat[0])).tolist()

    print(len(test_url(url)))

if __name__ == "__main__":
    import sys
    test(sys.argv[1])
