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

"""``main.py`` is the entry of programs, you can run ``python main.py``
Options:
    --port        Define the port your server run on
    --gpu         Define the GPU your server run on
    --model_name  Define the model you need
"""

import os
import json
import queue
import time
import signal
import requests
import threading
import shortuuid
import argparse
import traceback
import concurrent.futures
from multiprocessing import Queue, Process
from asyncio.futures import wrap_future
from concurrent.futures import ThreadPoolExecutor

import tornado
import tornado.web
import tornado.httpserver
from tornado import gen

import config
import util
import controller

_INSERT = "insert"
_SEARCH = "search"
_HEADERS = {"content-type": "application/json"}


class PackageProcess(Process):
    """The Process dealing request and return result"""

    def __init__(self, 
                 gpu_id: str,
                 model_name: str,
                 input_queue: Queue,
                 output_queue: Queue
                 ) -> None:
        Process.__init__(self)
        self.gpu_id = gpu_id               # The GPU id of model run on
        self.model_name = model_name       # image_retrieval or face_retrieval
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.watch_queue = queue.Queue()   # thread queue for performance

    def build(self):
        """Initialization process environment
            1.set visible GPU;
            2.load defined model
        """
        os.environ['CUDA_VISIBLE_DEVICES'] = self.gpu_id
        self.daemon_thread()
        self.model = controller.load_model(self.model_name)

    def daemon_thread(self):
        def kill():
            while True:
                if os.getppid() == 1:
                    os.kill(os.getpid(), signal.SIGKILL)
                time.sleep(1)
        t = threading.Thread(target=kill)
        t.start()

    def package_body(self,
                     method:str,
                     data:dict
                     ) -> None:
        """Package and deal request ,return response"""
        # get the name of DB and Space
        db_name = data.pop("db")
        space_name = data.pop("space")

        ip = f"{config.router_address}/{db_name}/{space_name}"
        if method == _INSERT:
            # define id for request
            if "_id" in data:
                _id = data.pop("_id")
                ip = f"{ip}/{_id}"
            result = self.model.insert(ip, data)
        else:
            # define the num of return
            size = data.pop("size", 10)
            ip = f"{ip}/_search?size={size}"
            result = self.model.search(ip, data)
        return result

    def deal(self, param):
        # Repackage the request body and send it to VectorDB
        # and put the response into `response_queue`
        try:
            uuid, method, data = param
            result = self.package_body(method, data)
            self.output_queue.put((uuid, True, result))
        except Exception as err:
            self.output_queue.put((uuid, False, str(err)))

    def run(self):
        # executor = ThreadPoolExecutor(20)
        self.build()
        print("load model success")

        while True:
            param = self.input_queue.get()
            self.deal(param)
            # executor.submit(self.deal, param)


def set_result_thread(result_queue, futures_dict):
    while True:
        uuid, flag, result = result_queue.get()
        if uuid in futures_dict:
            if not flag:
                result = json.dumps(
                    {
                        "status": 550,
                        "error": {
                            "index": "",
                            "index_uuid": "",
                            "shard": "0",
                            "type": "",
                            "reason": result
                        }
                    }
                )
            futures_dict[uuid].set_result(result)
            del futures_dict[uuid]


class TableHandler(tornado.web.RequestHandler):

    def initialize(self, input_queue, futures_dict):
        self.input_queue = input_queue
        self.futures_dict = futures_dict
        logger.info(f"request -*- {self.request.uri} -*- {self.request.body.decode('utf8')}")

    def parse_uri(self):
        self.uri = self.request.uri.split("?")[0].split("/")
        self.db_name, self.space_name, self.operate = self.uri[1:4]

    def write(self, chunk):
        super().write(chunk)
        logger.debug(f"response -*- {self.request.uri} -*- {chunk}")

    async def get(self):
        response = requests.get(f"{config.router_address}/{self.request.uri}", headers=_HEADERS)
        self.write(response.text)

    async def delete(self):
        response = requests.delete(f"{config.router_address}/{self.request.uri}", headers=_HEADERS)
        data = response.json()
        if data["status"] == 200:
            result = {"code": 200, "msg": "success"}
        else:
            result = {"code": data["status"], "msg": data["error"]["reason"]}

        self.write(json.dumps(result))

    async def put(self):
        method = "PUT"
        await self.deal_operate(method)

    async def post(self):
        method = "POST"
        await self.deal_operate(method)

    async def deal_operate(self, method):
        try:
            self.parse_uri()
            # print(self.request.body)
            if self.request.body:
                request_body = json.loads(self.request.body)
            else:
                request_body = None
            if self.operate == "_create":
                message = self._create(request_body)
            elif self.operate == "_delete":
                message = self._delete(request_body)
            elif self.operate == "_search":
                message = await self._search(request_body)
            elif self.operate == "_detect":
                message = await self._detect(request_body)
            elif self.operate == "_update":
                request_body["_id"] = self.get_argument("id")
                message = await self._insert(request_body)
            elif self.operate == "_insert":
                message = await self._insert(request_body)
            else:
                message = "Request is invalid"

        except Exception as err:
            err_info = util.catch_exc()
            logger.debug(err_info)
            message = str(err)
        finally:
            self.write(message)

    async def deal(self, method, data):
        data["db"] = self.db_name
        data["space"] = self.space_name
        uuid = shortuuid.uuid()
        future = concurrent.futures.Future()
        self.futures_dict[uuid] = future
        self.input_queue.put((uuid, method, data))
        result = await wrap_future(future)
        return result

    def _create(self, data):
        '''Create Database and Space
        Args:
            data: Dict
        '''

        db_flag = data.pop("db", True)
        if db_flag:
            res = requests.put(
                        config.master_address + "/db/_create",
                        headers=_HEADERS,
                        data=json.dumps({"name": self.db_name})
                    )
            result_db = json.loads(res.text)
            if result_db["code"] != 200:
                return result_db
        else:
            result_db = {"msg": None}

        # create space
        feature_default = {"type": "vector",
                           "model_id": "vgg16",
                           "dimension": 512,
                           "filed": "imageurl"
                           }
        columns_default = {"imageurl": {"type": "keyword"},
                           "boundingbox": {"type": "keyword"},
                           "label": {"type": "keyword"}
                           }

        method = data.pop("method", "innerproduct")
        feature = data.pop("feature", feature_default)
        columns = data.pop("columns", columns_default)

        # columns
        assert "label" in columns, "label not in columns"
        assert "imageurl" in columns, "imageurl not in columns"
        assert "boundingbox" in columns, "boundingbox not in columns"

        # feature
        assert "model_id" in feature, "model_id not in feature"
        assert "dimension" in feature, "dimension not in feature"

        data = {
                "name": self.space_name,
                "engine": {"name": "gamma", "metric_type": method},
                "properties": {}
                }

        data["properties"].update(columns)
        data["properties"]["feature"] = feature

        res = requests.put(
                    f"{config.master_address}/space/{self.db_name}/_create?timeout=600",
                    headers=_HEADERS,
                    data=json.dumps(data)
                )
        result_space = json.loads(res.text)
        if result_space["code"] != 200:
            return result_space

        result = {"code": 200, "db_msg": result_db["msg"], "space_msg": result_space["msg"]}
        return result

    def _delete(self, data):
        space_flag = data.pop("space", True)
        if space_flag:
            res = requests.delete(f"{config.master_address}/space/{self.db_name}/{self.space_name}")
            result_space = json.loads(res.text)
            if result_space["code"] != 200:
                return result_space
        else:
            result_space = {"msg": None}

        db_flag = data.pop("db", True)
        if db_flag:
            res = requests.delete(f"{config.master_address}/db/{self.db_name}")
            result_db = json.loads(res.text)
            if result_db["code"] != 200:
                return result_db
        else:
            result_db = {"msg": None}

        result = {"code": 200, "db_msg": result_db["msg"], "space_msg": result_space["msg"]}
        return result

    async def _search(self, data):
        response_body = await self.deal("search", data)
        return json.dumps(response_body)

    async def _insert(self, data):
        method = data.pop("method", "single")
        assert "imageurl" in data, "imageurl not in requests body"
        result = {"db": self.db_name, "space": self.space_name, "ids": [], "successful": 0}

        def pkg_result(response_body_list):
            assert isinstance(response_body_list, list), response_body_list
            for response_body in response_body_list:
                # assert "_id" in response_body, response_body
                if "_id" not in response_body:
                    continue
                _id = response_body["_id"]
                if response_body["status"] == 201:
                    result["ids"].append({_id: "successful"})
                    result["successful"] = result.get("successful") + 1
                else:
                    result["ids"].append({_id: response_body["error"]["reason"]})

        if method == "single":
            # url = data["imageurl"]
            response_body_list = await self.deal("insert", data)
            pkg_result(response_body_list)

        elif method == "bulk":
            imagefile = data.pop("imageurl")
            if not os.path.exists(imagefile):
                raise Exception(f"{imagefile} not exists!")
            with open(imagefile, 'r') as fr:
                col_names = fr.readline().strip().split(",")
                body_list = [dict(zip(col_names, col_values.strip().split(","))) for col_values in fr.readlines()]
            response_body_list = await gen.multi([self.deal("insert", data) for data in body_list])
            pkg_result([res[0] for res in response_body_list])

        return result


def parse_argument():
    parser = argparse.ArgumentParser()
    # parser.add_argument("--port", type=int, default=4101, help="Port the server run on")
    # parser.add_argument("--gpu", type=str, default="-1", help="Which GPU the server run on")
    parser.add_argument("--model_name", type=str, default="face_retrieval", help="The model name you need")
    args = parser.parse_args()
    return args


def run(port, url_queue, result_queue):
    futures_dict = dict()
    t1 = threading.Thread(target=set_result_thread, args=(result_queue, futures_dict))
    t1.start()
    app = tornado.web.Application(
        [(f'/.*', TableHandler, dict(input_queue=url_queue, futures_dict=futures_dict))]
    )
    sockets = tornado.netutil.bind_sockets(port)
    server = tornado.httpserver.HTTPServer(app)
    server.add_sockets(sockets)
    tornado.ioloop.IOLoop.instance().start()


def main():
    args = parse_argument()
    model_name = args.model_name
    # port = args.port
    # gpu = args.gpu
    port = config.port
    gpu = config.gpu
    url_queue = Queue()
    result_queue = Queue()
    package_process = PackageProcess(gpu, model_name, url_queue, result_queue)
    package_process.daemon = True
    package_process.start()

    run(port, url_queue, result_queue)

if __name__ == "__main__":
    logger = util.get_logger("main", "./log/request.log", level="DEBUG")
    main()



