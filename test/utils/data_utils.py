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
from utils.vearch_utils import logger


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


def download_from_irisa(host, dirname, local_dir, filename):
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    if os.path.isfile(local_dir + filename):
        logger.debug("%s exists, no need to download" % (local_dir + filename))
        return True
    ftp = FTP(host)
    ftp.login()
    ftp.set_pasv(True)
    ftp.cwd(dirname)

    with open(local_dir + filename, "wb") as local_file:
        ftp.retrbinary("RETR " + filename, local_file.write)
    ftp.quit()

    if os.path.isfile(local_dir + filename):
        logger.debug("%s successful download" % (local_dir + filename))
        return True
    else:
        logger.error("%s download failed" % (local_dir + filename))
        return False

def download_from_github(url: str, local_dir: str, filename: str, max_retries: int = 3) -> bool:
    """Download file from GitHub with retry mechanism and file integrity verification
    
    Args:
        url: GitHub download URL
        local_dir: Local directory to save file
        filename: Name of the file to save
        max_retries: Maximum number of retry attempts
    
    Returns:
        bool: True if successful, False otherwise
    """
    import requests
    import time
    import tarfile
    from tqdm import tqdm
    
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
        
    fname = os.path.join(local_dir, filename)
    
    for attempt in range(max_retries):
        try:
            # Stream download
            with requests.get(url, stream=True, timeout=30) as response:
                response.raise_for_status()
                
                with open(fname, 'wb') as f:
                    for chunk in tqdm(
                        response.iter_content(chunk_size=8192),
                        desc=f'Downloading {filename} (attempt {attempt + 1}/{max_retries})'
                    ):
                        if chunk:
                            f.write(chunk)
                            
            # Verify if file is a valid tar.gz
            try:
                with tarfile.open(fname, 'r:gz') as tar:
                    tar.getmembers()  # Verify file content
                logger.info(f"Successfully downloaded: {filename}")
                return True
            except Exception as e:
                logger.error(f"File is corrupted: {e}")
                os.remove(fname)
                raise Exception("Invalid file format")
                
        except Exception as e:
            logger.error(f"Download failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if os.path.exists(fname):
                os.remove(fname)
            if attempt < max_retries - 1:
                wait_time = 5 * (attempt + 1)  # Incremental wait time
                logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
    
    logger.error(f"Download failed after {max_retries} attempts")
    return False


def untar(fname, dirs, untar_result_dirs):
    if not os.path.exists(dirs):
        os.makedirs(dirs)
    if not os.path.isfile(os.path.join(dirs, fname)):
        logger.debug("%s not exist, cann't untar" % (fname))
        return
    if os.path.exists(os.path.join(dirs, untar_result_dirs)):
        logger.debug("%s exist, no need to untar" % os.path.join(dirs, untar_result_dirs))
        return
    t = tarfile.open(os.path.join(dirs, fname))
    t.extractall(path=dirs)


def load_sift1M():
    xt = fvecs_read("sift/sift_learn.fvecs")
    xb = fvecs_read("sift/sift_base.fvecs")
    xq = fvecs_read("sift/sift_query.fvecs")
    gt = ivecs_read("sift/sift_groundtruth.ivecs")
    logger.debug("successful load sift1M")
    return xb, xq, xt, gt


def load_sift10K():
    xt = fvecs_read("siftsmall/siftsmall_learn.fvecs")
    xb = fvecs_read("siftsmall/siftsmall_base.fvecs")
    xq = fvecs_read("siftsmall/siftsmall_query.fvecs")
    gt = ivecs_read("siftsmall/siftsmall_groundtruth.ivecs")
    logger.debug("successful load sift10K")
    return xb, xq, xt, gt


def get_sift1M():
    url = "ftp://ftp.irisa.fr"
    dirname = "local/texmex/corpus/"
    filename = "sift.tar.gz"
    host = get_ftp_ip(url)
    if download_from_irisa(host, dirname, "./", filename) == False:
        return
    untar(filename, "./", "sift")
    xb, xq, xt, gt = load_sift1M()
    return xb, xq, xt, gt


def normalization(data):
    data[np.linalg.norm(data, axis=1) == 0] = 1.0 / np.sqrt(data.shape[1])
    data /= np.linalg.norm(data, axis=1)[:, np.newaxis]
    return data


class Dataset:
    def __init__(self):
        self.d = -1
        self.metric = "L2"  # or InnerProduct
        self.nq = -1
        self.nb = -1
        self.nt = -1
        self.url = ""
        self.basedir = ""

        self.download()

    def download(self):
        pass

    def get_database(self):
        pass

    def get_queries(self):
        pass

    def get_groundtruth(self):
        pass


class DatasetSift10K(Dataset):
    """
    Data from ftp://ftp.irisa.fr/local/texmex/corpus/siftsmall.tar.gz
    """

    def __init__(self):
        self.d = 128
        self.metric = "L2"
        self.nq = 100
        self.nb = 10000
        self.nt = -1
        self.url = "ftp://ftp.irisa.fr"
        self.basedir = "datasets/"

        self.download()

    def download(self):
        # dirname = "local/texmex/corpus/"
        # filename = "siftsmall.tar.gz"
        # host = get_ftp_ip(self.url)
        # if (
        #     download_from_irisa(host, dirname, self.basedir, filename)
        #     == False
        # ):
        #     return
        # untar(filename, self.basedir, "siftsmall")
        filename = "v0.0.1.tar.gz"
        download_from_github("https://github.com/vearch/sift/archive/refs/tags/v0.0.1.tar.gz", "datasets", filename)

        untar(filename, "datasets", "v0.0.1")
        shutil.move("datasets/sift-0.0.1/siftsmall.tar.gz", "datasets/siftsmall.tar.gz")
        # remove the extracted files
        shutil.rmtree("datasets/sift-0.0.1")
        # remove v0.0.1
        os.remove("datasets/v0.0.1.tar.gz")
        untar("siftsmall.tar.gz", "./datasets", "siftsmall")

    def get_database(self):
        return fvecs_read(self.basedir + "siftsmall/siftsmall_base.fvecs")

    def get_queries(self):
        return fvecs_read(self.basedir + "siftsmall/siftsmall_query.fvecs")

    def get_groundtruth(self):
        return ivecs_read(self.basedir + "siftsmall/siftsmall_groundtruth.ivecs")


class DatasetSift1M(Dataset):
    """
    Data from ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz
    """

    def __init__(self):
        self.d = 128
        self.metric = "L2"
        self.nq = 10000
        self.nb = 10**6
        self.nt = -1
        self.url = "ftp://ftp.irisa.fr"
        self.basedir = "datasets/"

        self.download()

    def download(self):
        dirname = "local/texmex/corpus/"
        filename = "sift.tar.gz"
        host = get_ftp_ip(self.url)
        if (
            download_from_irisa(host, dirname, self.basedir, filename)
            == False
        ):
            return
        untar(filename, self.basedir, "sift")

    def get_database(self):
        return fvecs_read(self.basedir + "sift/sift_base.fvecs")

    def get_queries(self):
        return fvecs_read(self.basedir + "sift/sift_query.fvecs")

    def get_groundtruth(self):
        return ivecs_read(self.basedir + "sift/sift_groundtruth.ivecs")


class DatasetGlove(Dataset):
    """
    Data from http://ann-benchmarks.com/glove-100-angular.hdf5
    """

    def __init__(self):
        import h5py

        self.metric = "IP"
        self.d, self.nt = 100, 0

        self.url = "http://ann-benchmarks.com/glove-100-angular.hdf5"
        self.basedir = "datasets/glove/"
        self.download()

        self.glove_h5py = h5py.File(self.basedir + "glove-100-angular.hdf5", "r")
        self.nb = self.glove_h5py["train"].shape[0]
        self.nq = self.glove_h5py["test"].shape[0]

    def download(self):
        import requests

        fname = self.basedir + "glove-100-angular.hdf5"
        if os.path.isfile(fname):
            logger.debug("%s exists, no need to download" % (fname))
            return
        if not os.path.exists(self.basedir):
            os.makedirs(self.basedir)
        response = requests.get(self.url)
        if response.status_code == 200:
            with open(fname, "wb") as file:
                file.write(response.content)
        else:
            logger.error(
                f"Failed to download file. Response status code: {response.status_code}"
            )

    def get_queries(self):
        xq = np.array(self.glove_h5py["test"])
        return normalization(xq)

    def get_database(self):
        xb = np.array(self.glove_h5py["train"])
        return normalization(xb)

    def get_groundtruth(self):
        return self.glove_h5py["neighbors"]


class DatasetGlove25(Dataset):
    """
    Data from http://ann-benchmarks.com/glove-25-angular.hdf5
    """

    def __init__(self):
        import h5py

        self.metric = "IP"
        self.d, self.nt = 25, 0

        self.url = "http://ann-benchmarks.com/glove-25-angular.hdf5"
        self.basedir = "datasets/glove/"
        self.download()

        self.glove_h5py = h5py.File(self.basedir + "glove-25-angular.hdf5", "r")
        self.nb = self.glove_h5py["train"].shape[0]
        self.nq = self.glove_h5py["test"].shape[0]

    def download(self):
        import requests

        fname = self.basedir + "glove-25-angular.hdf5"
        if os.path.isfile(fname):
            logger.debug("%s exists, no need to download" % (fname))
            return
        if not os.path.exists(self.basedir):
            os.makedirs(self.basedir)
        response = requests.get(self.url)
        if response.status_code == 200:
            with open(fname, "wb") as file:
                file.write(response.content)
        else:
            logger.error(
                f"Failed to download file. Response status code: {response.status_code}"
            )

    def get_queries(self):
        xq = np.array(self.glove_h5py["test"])
        return normalization(xq)

    def get_database(self):
        xb = np.array(self.glove_h5py["train"])
        return normalization(xb)

    def get_groundtruth(self):
        return self.glove_h5py["neighbors"]


class DatasetNytimes(Dataset):
    """
    Data from http://ann-benchmarks.com/nytimes-256-angular.hdf5
    """

    def __init__(self):
        import h5py

        self.metric = "IP"
        self.d, self.nt = 100, 0

        self.url = "http://ann-benchmarks.com/nytimes-256-angular.hdf5"
        self.basedir = "datasets/nytimes/"
        self.download()

        self.nytimes_h5py = h5py.File(self.basedir + "nytimes-256-angular.hdf5", "r")
        self.nb = self.nytimes_h5py["train"].shape[0]
        self.nq = self.nytimes_h5py["test"].shape[0]

    def download(self):
        import requests

        fname = self.basedir + "nytimes-256-angular.hdf5"
        if os.path.isfile(fname):
            logger.debug("%s exists, no need to download" % (fname))
            return
        if not os.path.exists(self.basedir):
            os.makedirs(self.basedir)
        response = requests.get(self.url)
        if response.status_code == 200:
            with open(fname, "wb") as file:
                file.write(response.content)
        else:
            logger.error(
                f"Failed to download file. Response status code: {response.status_code}"
            )

    def get_queries(self):
        xq = np.array(self.nytimes_h5py["test"])
        return normalization(xq)

    def get_database(self):
        xb = np.array(self.nytimes_h5py["train"])
        if xb.dtype != np.float32:
            xb = xb.astype(np.float32)
        return normalization(xb)

    def get_groundtruth(self):
        return self.nytimes_h5py["neighbors"]


class DatasetMusic1M(Dataset):
    """
    Data from https://github.com/stanis-morozov/ip-nsw#dataset
    """

    def __init__(self):
        Dataset.__init__(self)
        self.d, self.nt, self.nb, self.nq = 100, 0, 10**6, 10000
        self.metric = "IP"
        self.basedir = "datasets/music/"

    def download(self):
        # TODO
        pass

    def get_database(self):
        xb = np.fromfile(self.basedir + "database_music100.bin", dtype="float32")
        xb = xb.reshape(-1, 100)
        return xb

    def get_queries(self):
        xq = np.fromfile(self.basedir + "query_music100.bin", dtype="float32")
        xq = xq.reshape(-1, 100)
        return xq

    def get_groundtruth(self):
        return np.load(self.basedir + "gt.npy")


class DatasetGist1M(Dataset):
    """
    Data from ftp://ftp.irisa.fr/local/texmex/corpus/gist.tar.gz
    """

    def __init__(self):
        self.d = 960
        self.metric = "L2"
        self.nq = 1000
        self.nb = 10**6
        self.nt = -1
        self.url = "ftp://ftp.irisa.fr"
        self.basedir = "datasets/"

        self.download()

    def download(self):
        dirname = "local/texmex/corpus/"
        filename = "gist.tar.gz"
        host = get_ftp_ip(self.url)
        if (
            download_from_irisa(host, dirname, self.basedir, filename)
            == False
        ):
            return
        untar(filename, self.basedir, "gist")

    def get_database(self):
        return fvecs_read(self.basedir + "gist/gist_base.fvecs")

    def get_queries(self):
        return fvecs_read(self.basedir + "gist/gist_query.fvecs")

    def get_groundtruth(self):
        return ivecs_read(self.basedir + "gist/gist_groundtruth.ivecs")


def get_dataset_by_name(name):
    if name == "sift":
        dataset = DatasetSift1M()
        return dataset.get_database(), dataset.get_queries(), dataset.get_groundtruth()
    elif name == "siftsmall":
        dataset = DatasetSift10K()
        return dataset.get_database(), dataset.get_queries(), dataset.get_groundtruth()
    elif name == "glove":
        dataset = DatasetGlove()
        return dataset.get_database(), dataset.get_queries(), dataset.get_groundtruth()
    elif name == "glove25":
        dataset = DatasetGlove25()
        return dataset.get_database(), dataset.get_queries(), dataset.get_groundtruth()
    elif name == "nytimes":
        dataset = DatasetNytimes()
        return dataset.get_database(), dataset.get_queries(), dataset.get_groundtruth()
    elif name == "gist":
        dataset = DatasetGist1M()
        return dataset.get_database(), dataset.get_queries(), dataset.get_groundtruth()
