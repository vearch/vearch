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

#Definition of parameters

# Define which port the web runs on
# you can view the result of searching by
# inputting http://yourIP:show_port in your web browse
port = 4101
# Batch Extraction Features, you can modify batch_size by
# your GPU Memory
batch_size = 16
# The name of model for web platform, Currently only one model is supported
detect_model = "yolo3"
extract_model = "vgg16"
# Define gpu parameters,
gpu = "0"
# Define deployment address of vearch
ip_address = "http://****"
ip_scheme = ip_address + ":443/space"
ip_insert = ip_address + ":80"
database_name = "test"
table_name = "test"
