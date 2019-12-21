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

"""``main.py`` is the entry of programs, you can run ``python main.py``.
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
import tornado
import tornado.web
import tornado.httpserver
from tornado.httputil import HTTPServerRequest
from typing import Dict, List, Union
from tornado.options import define, options

import config
import util
import controller

_INSERT = '_insert'
_SEARCH = '_search'
_HEADERS = {'content-type': 'application/json'}
# InvalidURL = requests.InvalidURL


class PackageProcess(Process):
    """The Process dealing request and return result."""

    def __init__(self, 
                 gpu_id: str,
                 model_name: str,
                 input_queue: Queue,
                 output_queue: Queue,
                 debug: bool = False
                 ) -> None:
        Process.__init__(self)
        self.gpu_id = gpu_id               # The GPU id of model run on
        self.model_name = model_name       # image_retrieval or face_retrieval
        self.model = None
        self.debug = debug
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.watch_queue = queue.Queue()   # thread queue for performance

    def build(self):
        """Initialization process environment.
            1.set visible GPU;
            2.load defined model
        """
        os.environ['CUDA_VISIBLE_DEVICES'] = self.gpu_id
        self.daemon_thread()
        self.model = controller.load_model(self.model_name)

    @staticmethod
    def daemon_thread():
        """Define a daemon thread listen the status of parent process."""
        def kill():
            while True:
                if os.getppid() == 1:
                    os.kill(os.getpid(), signal.SIGKILL)
                time.sleep(1)
        t = threading.Thread(target=kill)
        t.start()

    @staticmethod
    def judge(uri_list: List[str], request: 'Request') -> None:
        """Raise a Exception when create a db that name in [_cluster, list, db, space]
        :param uri_list: The list of uri.
        :param request: The dict of request body.
        :return None
        :raise Exception
        """
        if uri_list[1] == 'db' and uri_list[2] == '_create':
            if not request.body:
                raise requests.RequestException('The request data can not be empty!')
            data = json.loads(request.body, encoding='utf8')
            if 'name' not in data:
                raise requests.RequestException('The name of db is required in request data!')
            if data['name'] in ['_cluster', 'list', 'db', 'space']:
                raise requests.RequestException('The name of db can not in [_cluster, list, db, space]!')

    def deal(self, uuid: str, request: 'Request') -> None:
        """Deal the request.
        Repackage the request body and send it to VectorDB and put the response into `response_queue`
        :param uuid: The uuid of request.
        :param request: The request object.
        :return None
        """
        try:
            uri_list = request.uri.split('?')[0].split('/')
            if len(uri_list) < 3:
                raise requests.RequestException('Bad Request, Page not Found.')
            elif uri_list[1] in ['_cluster', 'list', 'db', 'space']:
                self.judge(uri_list, request)
                ip = f'{config.master_address}{request.uri}'
            elif request.method == 'POST':
                self.post(uri_list, request)
                ip = f'{config.router_address}{request.uri}'
            else:
                ip = f'{config.router_address}{request.uri}'
            res = requests.request(request.method, ip, data=request.body, headers=request.headers)
            result = Response(res)
            self.output_queue.put((uuid, True, result))
        except Exception as err:
            if self.debug:
                traceback.print_exc()
            self.output_queue.put((uuid, False, str(err)))

    def post(self, uri_list: List[str], request: 'Request') -> None:
        """Extract feature in request if necessary.
        :param uri_list: The list of uri.
        :param request: The dict of request body.
        :return None
        :raise NotImplementedError
        """
        flag = False
        operate = uri_list[3] if len(uri_list) >= 4 else None
        data = json.loads(request.body, encoding='utf8')
        if operate == _SEARCH:
            for d in data['query']['sum']:
                if not isinstance(d['feature'], list):
                    flag = True
                    d['feature'] = util.normlize(self.model.encode(d['feature']))
        elif operate == '_msearch':
            raise NotImplementedError('This API do not support msearch operate')
        else:
            for key in data:
                if isinstance(data[key], dict) and 'feature' in data[key]:
                    field = data[key]['feature']
                    if not isinstance(field, list):
                        flag = True
                        data[key]['feature'] = util.normlize(self.model.encode(field))
        if flag:
            # if not extract feature, send the request to server directly.
            request.body = json.dumps(data, ensure_ascii=False).encode('utf8')

    def run(self):
        """The entry to program start."""
        self.build()
        print('load model success')
        while True:
            uuid, request = self.input_queue.get()
            self.deal(uuid, request)


class TableHandler(tornado.web.RequestHandler):
    """The server of receive and deal request."""

    def initialize(self, input_queue: Queue,
                   futures_dict: Dict[str, concurrent.futures.Future]
                   ) -> None:
        """Initialize the request, called for each request.
        :param input_queue: The request queue.
        :param futures_dict: The future dict store the uuid of request and future.
        """
        self.input_queue = input_queue
        self.futures_dict = futures_dict

    def write(self, result: Union[str, 'Response']):
        if isinstance(result, Response):
            code, chunk = result.status_code, result.text
        else:
            code, chunk = 551, result
        self.set_status(code, reason=chunk)
        # chunk = json.dumps(result, ensure_ascii=False)
        # status = result.get('status', 0) or result.get('code', 0)
        # if status:
        #     self.set_status(status, reason=chunk)
        super(TableHandler, self).write(chunk)

    async def get(self):
        response = await self.deal()
        self.write(response)

    async def delete(self):
        response = await self.deal()
        self.write(response)

    async def put(self):
        response = await self.deal()
        self.write(response)

    async def post(self):
        response = await self.deal()
        self.write(response)

    async def deal(self):
        uuid = shortuuid.uuid()
        future = concurrent.futures.Future()
        self.futures_dict[uuid] = future
        self.input_queue.put((uuid, Request(self.request)))
        result = await wrap_future(future)
        return result


class Request(dict):
    def __init__(self, a: HTTPServerRequest):
        super(Request, self).__init__()
        self.method = a.method
        self.uri = a.uri
        self.headers = a.headers
        self.body = a.body

    def __str__(self):
        return f'{self.__class__.__name__}({self.__dict__})'

    def __repr__(self):
        return self.__str__()


class Response(dict):
    def __init__(self, a: requests.Response):
        super(Response, self).__init__()
        self.url = a.url
        self.text = a.text
        self.elapsed = a.elapsed
        self.status_code = a.status_code

    def __str__(self):
        return f'{self.__class__.__name__}({self.__dict__})'

    def __repr__(self):
        return self.__str__()


def set_result_thread(result_queue: Queue,
                      futures_dict: Dict[str, concurrent.futures.Future]
                      ) -> None:
    """Assign results asynchronously.
    :param result_queue: The queue of result which put by ``PackageProcess``.
    :param futures_dict: The dict which save future of each request.
    :return None.
    """
    while True:
        # uuid is the id of request.
        # flag is the status of request, True is success other is failed.
        # result is the result of request, if flag is false, result is the error info of request.
        uuid, flag, result = result_queue.get()
        if uuid in futures_dict:
            futures_dict[uuid].set_result(result)
            del futures_dict[uuid]


def run(port, url_queue, result_queue):
    tornado.options.parse_command_line()
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


def install(model_name):
    if model_name == 'face_retrieval':
        util.install_package('tensorflow-gpu==1.15.0 mtcnn keras==2.2.4')
    elif model_name == 'image_retrieval':
        util.install_package('torchvision torch')
    elif model_name == 'text':
        text_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'text', 'requirements.txt')
        util.install_package(f'-r {text_path}')
    elif model_name == 'audio':
        raise NotImplementedError()
    else:
        raise Exception(f'{model_name} is not existed')


def main():
    define('model_name', type=str, default='face_retrieval', help='The model name you need')
    define('debug', type=bool, default=False, help='debug')
    tornado.options.parse_command_line()
    install(options.model_name)
    port = config.port
    gpus = config.gpus
    url_queue = Queue()
    result_queue = Queue()
    for gpu in gpus.split(','):
        package_process = PackageProcess(gpu, options.model_name, url_queue, result_queue, debug=options.debug)
        package_process.daemon = True
        package_process.start()

    run(port, url_queue, result_queue)


if __name__ == '__main__':
    main()



