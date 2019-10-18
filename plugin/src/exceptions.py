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


class LoadModelError(Exception):
    def __init__(self, message):
        super.__init__(message)
        self.message = message

class InstallError(Exception):
    def __init__(self, message):
        super.__init__(message)
        self.message = message

class CreateDBAndSpaceError(Exception):
    def __init__(self, message="Create DB or Space Failed"):
        super.__init__(message)
        self.message = message

class ImageError(Exception):
    def __init__(self, message="Image path is invalid!"):
        super.__init__(message)
        self.message = message
        self.code = 404
