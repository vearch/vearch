#
# Copyright 2019 The Vearch Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

# -*- coding: UTF-8 -*-

import requests
import os
import shutil
import tarfile
from ftplib import FTP
from urllib.parse import urlparse
import socket
import numpy as np
import json
import time
from multiprocessing import Pool as ThreadPool


ip = "127.0.0.1"
ip_master = ip + ":8817"
ip_router = ip + ":9001"
router_url = "http://" + ip_router
db_name = "ts_db"
space_name = "ts_space"


__description__ = """ test utils for vearch """


def get_ftp_ip(url):
    parsed_url = urlparse(url)
    ftp_host = parsed_url.hostname
    ip_address = socket.gethostbyname(ftp_host)

    return ip_address

def ivecs_read(fname):
    a = np.fromfile(fname, dtype='int32')
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:].copy()


def fvecs_read(fname):
    return ivecs_read(fname).view('float32')


def download_sift(logger, host, dirname, filename):
    if os.path.isfile(filename):
        logger.debug("%s exists, no need to download" % (filename))
        return True
    ftp = FTP(host)
    ftp.login()
    ftp.set_pasv(True)
    ftp.cwd(dirname)

    with open(filename, 'wb') as local_file:
        ftp.retrbinary('RETR ' + filename, local_file.write)
    ftp.quit()

    if os.path.isfile(filename):
        logger.debug("%s successful download" % (filename))
        return True
    else:
        logger.error("%s download failed" % (filename))
        return False


def untar(fname, dirs):
    t = tarfile.open(fname)
    t.extractall(path = dirs) 

def load_sift1M(logger):
    xt = fvecs_read("sift/sift_learn.fvecs")
    xb = fvecs_read("sift/sift_base.fvecs")
    xq = fvecs_read("sift/sift_query.fvecs")
    gt = ivecs_read("sift/sift_groundtruth.ivecs")
    logger.debug("successful load sift1M")
    return xb, xq, xt, gt


def load_sift10K(logger):
    xt = fvecs_read("siftsmall/siftsmall_learn.fvecs")
    xb = fvecs_read("siftsmall/siftsmall_base.fvecs")
    xq = fvecs_read("siftsmall/siftsmall_query.fvecs")
    gt = ivecs_read("siftsmall/siftsmall_groundtruth.ivecs")
    logger.debug("successful load sift10K")
    return xb, xq, xt, gt


def get_sift1M(logger):
    url = "ftp://ftp.irisa.fr"
    dirname = "local/texmex/corpus/"
    filename = "sift.tar.gz"
    host = ftp_ip = get_ftp_ip(url)
    if download_sift(logger, host, dirname, filename) == False:
        return
    untar(filename, "./")
    xb, xq, xt, gt = load_sift1M(logger)
    return xb, xq, xt, gt


def get_sift10K(logger):
    url = "ftp://ftp.irisa.fr"
    dirname = "local/texmex/corpus/"
    filename = "siftsmall.tar.gz"
    host = ftp_ip = get_ftp_ip(url)
    if download_sift(logger, host, dirname, filename) == False:
        return
    untar(filename, "./")
    xb, xq, xt, gt = load_sift10K(logger)
    return xb, xq, xt, gt


def process_data(items):
    url = router_url + "/document/upsert"
    data = {}
    data["db_name"] = db_name
    data["space_name"] = space_name
    data["documents"] = []
    index = items[0]
    batch_size = items[1]
    features = items[2]
    for j in range(batch_size):
        param_dict = {}
        param_dict['field_int'] = index * batch_size + j
        param_dict['field_vector'] = {
            "feature": features[j].tolist()
        }
        data["documents"].append(param_dict)
    
    json_str = json.dumps(data)
    rs = requests.post(url, json_str)


def add(total, batch_size, xb):
    pool = ThreadPool()
    total_data = []
    for i in range(total):
        total_data.append((i, batch_size,  xb[i * batch_size: (i + 1) * batch_size]))
    results = pool.map(process_data, total_data)
    pool.close()
    pool.join()

def waiting_index_finish(logger, total):
    url = router_url + "/_cluster/health?db=" + db_name + "&space=" + space_name
    num = 0
    while num < total:
        response = requests.get(url)
        num = response.json()[0]["spaces"][0]["partitions"][0]["index_num"]
        logger.info("index num: %d" %(num))
        time.sleep(10)
        
def search(xq, k:int, batch:bool, query_dict:dict, logger):
    url = router_url + "/document/search?timeout=2000000"

    field_ints = []
    vector_dict = {
        "vector": [{
            "field": "field_vector",
            "feature": [],
        }]
    }
    if batch:
        vector_dict["vector"][0]["feature"] = xq.flatten().tolist()
        query_dict["query"]["vector"] = vector_dict["vector"]
        json_str = json.dumps(query_dict)
        rs = requests.post(url, json_str)
        #if rs.json()['code'] != 200:
        #    logger.info(rs.json())

        for results in rs.json()["documents"]:
            field_int = []
            for result in results:
                field_int.append(result["_source"]["field_int"])
            field_ints.append(field_int)
    else:
        for i in range(xq.shape[0]):
            vector_dict["vector"][0]["feature"] = xq[i].tolist()
            query_dict["query"]["vector"] = vector_dict["vector"]
            json_str = json.dumps(query_dict)
            rs = requests.post(url, json_str)

            field_int = []
            for results in rs.json()["documents"]:
                for result in results:
                    field_int.append(result["_source"]["field_int"])
            assert len(field_int) == k
            field_ints.append(field_int)
    assert len(field_ints) == xq.shape[0]  
    return np.array(field_ints)
    

def evaluate(xq, gt, k, batch, query_dict, logger):
    nq = xq.shape[0]
    t0 = time.time()
    I = search(xq, k, batch, query_dict, logger)
    t1 = time.time()

    recalls = {}
    i = 1
    while i <= k:
        recalls[i] = (I[:, :i] == gt[:, :1]).sum() / float(nq)
        i *= 10

    return (t1 - t0) * 1000.0 / nq, recalls


def drop_db(router_url: str, db_name: str):
    url = f'{router_url}/db/{db_name}'
    resp = requests.delete(url)
    return resp.text


def drop_space(router_url: str, db_name: str, space_name: str):
    url = f'{router_url}/space/{db_name}/{space_name}'
    resp = requests.delete(url)
    return resp.text


def destroy(router_url: str, db_name: str, space_name: str):
    print(drop_space(router_url, db_name, space_name))
    print(drop_db(router_url, db_name))


def create_db(router_url: str, db_name: str):
    url = f'{router_url}/db/_create'
    data = {'name': db_name}
    resp = requests.put(url, json=data)
    return resp.json()


def create_space(router_url: str, db_name: str, space_config: dict):
    url = f'{router_url}/space/{db_name}/_create'
    resp = requests.put(url, json=space_config)
    return resp.json()