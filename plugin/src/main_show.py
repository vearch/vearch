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
import json
import base64
import requests
import argparse
import tornado
import tornado.web
from tornado import escape

resource_path = os.path.join(
    os.path.dirname((os.path.abspath(__file__))),
    "static")

class ImageShowHandler(tornado.web.RequestHandler):
    """tornado service"""
    def post(self):
        """recieve post request and deal"""
        self.deal_operate()

    def deal_operate(self):
        image_object = self.request.files.get("file")[0]
        image_bytes = image_object["body"]
        image_path = os.path.join(resource_path, "images", image_object["filename"])
        self.save_img(image_path, image_bytes)
        sim_results = self.search_similar(image_path)
        sim_url = self.parse_result(sim_results)
        result = dict(ori_url = image_object["filename"],
                      sim_url = sim_url)
        self.write(escape.json_encode(result))

    def save_img(self, image_path, image_bytes):
        if not os.path.exists(os.path.dirname(image_path)):
            os.makedirs(os.path.dirname(image_path))
        with open(image_path, "wb") as fw:
            fw.write(image_bytes)

    def parse_result(self,result):
        search_list = []
        for hit in result['hits']['hits']:
            source = hit.pop("_source")
            source["score"] = hit["_extra"]["vector_result"][0]["score"]
            search_list.append(source)
        return search_list

    def search_similar(self, image_path):
        """"""
        headers = {"content-type": "application/json"}
        data = {"imageurl": image_path, "size": args.num}
        ip = f"http://{args.ip}/{args.db}/{args.space}/_search"
        response = requests.post(ip, headers=headers, data = json.dumps(data))
        result = json.loads(response.text)
        return result

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.redirect("/static/index.html")

def main():
    app = tornado.web.Application([
        (r"/*", MainHandler),
        (r'/static/vdb/search', ImageShowHandler),
        (r"/static/(.*)", tornado.web.StaticFileHandler, {"path": resource_path}),
        ])
    sockets = tornado.netutil.bind_sockets(args.port)
    server = tornado.httpserver.HTTPServer(app)
    server.add_sockets(sockets)
    tornado.ioloop.IOLoop.instance().start()

def parse_argument():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", type=str, default="127.0.0.1:4101", help="Ip of your service")
    parser.add_argument("--port", type=str, default=4102, help="Port the server run on")
    parser.add_argument("--db", type=str, default="test", help="DB name")
    parser.add_argument("--space", type=str, default="test", help="Space name")
    parser.add_argument("--num", type=int, default=20, help="The num of result")
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = parse_argument()
    main()
