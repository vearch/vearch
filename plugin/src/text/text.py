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
import subprocess
import numpy as np
from keras_bert import extract_embeddings,get_checkpoint_paths,load_trained_model_from_checkpoint, load_vocabulary


MODEL_URL = 'https://storage.googleapis.com/hfl-rc/chinese-bert/chinese_wwm_L-12_H-768_A-12.zip'


class Text(object):
    def __init__(self, config):
        model_path = config["model_path"]
        if not os.path.exists(model_path):
            model_dir = os.path.dirname(model_path)
            if not os.path.exists(model_dir):
                os.makedirs(model_dir)
            subprocess.run(
                f"wget -P {model_dir} {MODEL_URL} && cd {model_dir} && unzip chinese_wwm_L-12_H-768_A-12.zip",
                shell=True)

        paths = get_checkpoint_paths(model_path)
        self.model = load_trained_model_from_checkpoint(
                        config_file=paths.config,
                        checkpoint_file=paths.checkpoint,
                        output_layer_num=1)
        self.vocabs = load_vocabulary(paths.vocab)

    def encode(self, texts):
        embeddings = extract_embeddings(self.model, texts, vocabs=self.vocabs)
        # result = [np.max(x, axis=0) for x in embeddings]
        result = [np.max(x, axis=0).tolist() for x in embeddings]
        return result

def load_model(config):
    return Text(config)

def test():
    import time
    model = load_model({"model_path": "../../model/chinese_L-12_H-768_A-12"})
    while True:
        try:
            print("input your question!\n")
            question = input()
            test(model, question.split("--"))
        except Exception as err:
            print(err)
    result = model.encode(texts)
    print(np.linalg.norm(np.array(result[0]) - np.array(result[1])))

if __name__ == "__main__":
    pass
