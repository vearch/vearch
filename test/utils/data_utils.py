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

import shutil
import tarfile
from ftplib import FTP
from urllib.parse import urlparse
import socket
import numpy as np
import os


__description__ = """ test data utils for vearch """

def get_ftp_ip(url):
    parsed_url = urlparse(url)
    ftp_host = parsed_url.hostname
    ip_address = socket.gethostbyname(ftp_host)

    return ip_address


def ivecs_read(fname):
    a = np.fromfile(fname, dtype="int32")
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:].copy()


def fvecs_read(fname):
    return ivecs_read(fname).view("float32")


def download_sift(logger, host, dirname, filename):
    if os.path.isfile(filename):
        logger.debug("%s exists, no need to download" % (filename))
        return True
    ftp = FTP(host)
    ftp.login()
    ftp.set_pasv(True)
    ftp.cwd(dirname)

    with open(filename, "wb") as local_file:
        ftp.retrbinary("RETR " + filename, local_file.write)
    ftp.quit()

    if os.path.isfile(filename):
        logger.debug("%s successful download" % (filename))
        return True
    else:
        logger.error("%s download failed" % (filename))
        return False


def untar(fname, dirs):
    t = tarfile.open(fname)
    t.extractall(path=dirs)


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