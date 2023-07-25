#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import time
import os
import json
import struct
import random
import numpy as np
import vearch

#read sift
def fvecs_read(fname):
    a = np.fromfile(fname, dtype='int32')
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:].copy().view('float32')

def ivecs_read(fname):
    a = np.fromfile(fname, dtype='int32')
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:].copy()

def fdat_read(fname, d):
    a = np.fromfile(fname, dtype='int32')
    return a.reshape(-1, d).copy().view('float32')

def mmap_bvecs(fname):
    x = np.memmap(fname, dtype='uint8', mode='r')
    d = x[:4].view('int32')[0]
    array = x.reshape(-1, d + 4)[:, 4:]
    return np.ascontiguousarray(array.astype('float32'))

def get_nearestneighbors_vearch(engine, xq, k, nprobe, is_batch):
    nq = xq.shape[0]
    elap = 0
    index = []
    if is_batch:
        engine.verbose = True
        query =  {
            "vector": [{
            "field": "feature",
            "feature": xq                                    # data type is numpy
            }],
            "fields":['key'],
            "retrieval_param":{"metric_type": "L2", "nprobe": nprobe},  # HNSW: {"efSearch": 64, "metric_type": "L2" }
            "topn":k
        }
        start = time.time()
        results = engine.search(query)
        elap = time.time() - start
        for result in results:
            for result_item in result["result_items"]:
                index.append(result_item["key"])
    else:
        for i in range(nq):
            query =  {
                "vector": [{
                    "field": "feature",
                    "feature": xq[i]                                    # data type is numpy
                }],
                "fields":['key'],
                "retrieval_param": {"parallel_on_queries": 0, "nprobe": nprobe},  # HNSW: {"efSearch": 64, "metric_type": "L2" }
                "topn":k
            }
            start = time.time()
            results = engine.search(query)
            elap = elap + (time.time() - start)
            for result in results:
                for result_item in result["result_items"]:
                    index.append(result_item["key"])

    #print(len(index))
    print('average: %.4f ms, QPS: %.4f' % (elap * 1000 / nq, 1 / (elap / nq)))
    return np.ascontiguousarray(np.asarray(index).reshape(-1, k))

def calculate_recall(I, gt):
    nq = I.shape[0]
    # compute 1-recall at ranks 1, 10, 100
    recalls = []
    
    for rank in 1, 10, 100:
        print('R@', end="")
        recall = (I[:, :rank] == gt[:, :1]).sum() / float(nq)
        recalls.append(recall)
        print('%d: %.4f ' % (rank, recall), end=" ")
    print("")

    return recalls

def evaluate_vearch(xb, xq, xt, gt):
    engine = vearch.Engine("files", 'logs')
    nlist = 2048
    m = 32
    table = {
        "name" : "test_table",
        "engine" : {
            "index_size": 100000,
            "retrieval_type": "IVFPQ",       
            "retrieval_param": {
                "metric_type": "L2",
                "ncentroids": nlist,
                "nsubvector": m,
                #"hnsw" : {
                #    "nlinks": 32,
                #    "efConstruction": 200,
                #    "efSearch": 64
                #},
                #"opq": {
                #    "nsubvector": 64
                #}
            }
        },
        "properties" : {
            #"_id":{                        #You usually don't need to specify. Vearch is automatically specified.
            #    "type": "integer",         
            #    "is_index": True
            #},
            "key": {
                "type": "int",
                "is_index": False
            },
            "feature":{
                "type": "vector",
                "index": True,
                "dimension": 128,
                "store_type": "MemoryOnly", 
                #"store_param": {
                #    "cache_size": 10000
                #}
            }
        }
    }
    response_code = engine.create_table(table)
    if response_code == 0:                    #response_code: 0, success; 1 failed.
        print("create table success")
    else:
        print("create table failed")

    doc_items = []

    for i in range(xb.shape[0]):
        profiles = {}
        profiles["key"] = i

        #The feature type supports numpy only.
        profiles["feature"] = xb[i,:]

        doc_items.append(profiles)
    start = time.time()
    #print(doc_items[0])
    engine.verbose = True
    docs_id = engine.add(doc_items)
    engine.verbose = False
    print("add complete, success num: %d, cost %.4f s" % (len(docs_id), time.time() - start))
    time.sleep(5)

    #'min_indexed_num' = xb.shape[0]. Indexing complete.
    indexed_num = 0
    while indexed_num != xb.shape[0]:
        indexed_num = engine.get_status()['min_indexed_num']
        time.sleep(0.5)
    print("engine status:",engine.get_status())

    k = gt.shape[1]
    #for nprobe in [1, 5, 10, 20, 40, 80, 160, 320]:
    for nprobe in [80]:
        print("nlist: %d, m: %d, nprobe: %d" % (nlist, m, nprobe))
        I = get_nearestneighbors_vearch(engine, xq, k, nprobe, int(sys.argv[2]))
        calculate_recall(I, gt)
   

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python xxx.py data_dir batch[0 or 1]")
        print("And get sift1m data first")
        sys.exit(0)
    xb = fvecs_read(sys.argv[1] + "/sift_base.fvecs")
    xq = fvecs_read(sys.argv[1] + "/sift_query.fvecs")
    xt = fvecs_read(sys.argv[1] + "/sift_learn.fvecs")
    gt = ivecs_read(sys.argv[1] + "/sift_groundtruth.ivecs")
    evaluate_vearch(xb, xq, xt, gt)

