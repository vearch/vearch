import argparse
import psutil
import os
import logging
import tarfile
from ftplib import FTP
from urllib.parse import urlparse
import socket
import numpy as np
import yaml
import math
from typing import Any, Dict


def get_cpu_count():
    logical_cpus = psutil.cpu_count(logical=True)

    # check container
    if os.path.exists("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"):
        with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "r") as f:
            cfs_quota_us = int(f.read())
        with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us", "r") as f:
            cfs_period_us = int(f.read())

        if cfs_quota_us > 0 and cfs_period_us > 0:
            container_cpus = cfs_quota_us // cfs_period_us
            return min(container_cpus, logical_cpus)

    return logical_cpus


log_levels = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,  # NOTSET is typically used to indicate that no specific logging level is set.
}


def str2bool(v: str):
    if isinstance(v, bool):
        return v
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")


DATASETS = ["sift", "siftsmall", "glove", "nytimes", "gist", "random"]


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--dimension",
        help="the embedding vector dimension",
        type=int,
        default=512,
    )
    parser.add_argument(
        "--batch-size",
        help="the batch size",
        type=int,
        default=100,
    )
    parser.add_argument(
        "--partition-num",
        help="the partition num",
        type=int,
        default=3,
    )
    parser.add_argument(
        "--replica-num",
        help="the replica num",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--pool-size",
        help="the process pool size",
        type=int,
        default=get_cpu_count(),
    )
    parser.add_argument(
        "--db",
        default="ts_db_benchmark",
        type=str,
        help="the database name",
    )
    parser.add_argument(
        "--space",
        default="ts_space_benchmark",
        type=str,
        help="the space name",
    )
    parser.add_argument(
        "--user",
        default="root",
        type=str,
        help="the user name",
    )
    parser.add_argument(
        "--password",
        default="secret",
        type=str,
        help="the password",
    )
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:9001",
        type=str,
        help="the vearch router url",
    )
    # NOT NOW: GPU, BINARYIVF
    parser.add_argument(
        "--index-type",
        default="FLAT",
        type=str,
        choices=["IVFPQ", "HNSW", "IVFFLAT", "FLAT"],
        help="the vector index type",
    )
    # parser.add_argument(
    #     "--picture",
    #     default=False,
    #     type=bool,
    #     help="draw picture or not",
    # )
    # parser.add_argument(
    #     "--runs",
    #     metavar="COUNT",
    #     default=1,
    #     type=int,
    #     help="run each task %(metavar)s times and use only the best result",
    # )
    parser.add_argument(
        "--dataset",
        default="random",
        type=str,
        choices=DATASETS,
        help="the dataset to load",
    )
    parser.add_argument(
        "--limit",
        default=100,
        type=int,
        help="the number of near neighbours to search for",
    )
    parser.add_argument(
        "--recall",
        default=True,
        type=str2bool,
        help="calculate recall or not",
        choices=[True, False],
    )
    parser.add_argument(
        "--nb",
        help="the total num of embedding",
        type=int,
        default=10000,
    )
    parser.add_argument(
        "--nq",
        default=100,
        type=int,
        help="the number of interface requests of [query, delete, search]",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        type=str,
        help="the log level",
        choices=log_levels.keys(),
    )
    parser.add_argument(
        "--vector-value",
        default=True,
        type=str2bool,
        help="whether return vector value of query",
        choices=[True, False],
    )
    parser.add_argument(
        "--output",
        help="the path of the output file, if not set it will be stdout",
        type=str,
        default="",
    )
    parser.add_argument("--index-params", help="the index params", type=str, default="")
    parser.add_argument(
        "--task",
        help="the task type",
        type=str,
        default="NORMAL",
        choices=["CRUD", "SEARCH", "NORMAL"],
    )
    parser.add_argument(
        "--trace",
        default=True,
        type=str2bool,
        help="the trace option for search or query",
        choices=[True, False],
    )
    parser.add_argument(
        "--waiting-index",
        default=True,
        type=str2bool,
        help="waiting train and build index finished",
        choices=[True, False],
    )
    parser.add_argument(
        "--keep-space",
        default=False,
        type=str2bool,
        help="keep space and db or not",
        choices=[True, False],
    )
    args = parser.parse_args()

    return args


def get_ftp_ip(url: str):
    parsed_url = urlparse(url)
    ftp_host = parsed_url.hostname
    ip_address = socket.gethostbyname(ftp_host)

    return ip_address


def ivecs_read(fname: str):
    a = np.fromfile(fname, dtype="int32")
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:].copy()


def fvecs_read(fname: str):
    return ivecs_read(fname).view("float32")


def evaluate(search: np.ndarray, gt: np.ndarray, k: int):
    nq = gt.shape[0]
    recalls = {}
    i = 1
    while i <= k:
        recalls[i] = (search[:, :i] == gt[:, :1]).sum() / float(nq)
        i *= 10

    return recalls


def download_from_irisa(
    logger: logging, host: str, dirname: str, local_dir: str, filename: str
):
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


def untar(logger: logging, fname: str, dirs: str, untar_result_dirs: str):
    if not os.path.exists(dirs):
        os.makedirs(dirs)
    if not os.path.isfile(dirs + fname):
        logger.debug("%s not exist, cann't untar" % (fname))
        return
    if os.path.exists(dirs + untar_result_dirs):
        logger.debug("%s exist, no need to untar" % (dirs + untar_result_dirs))
        return
    t = tarfile.open(dirs + fname)
    t.extractall(path=dirs)


def normalization(data: np.ndarray):
    data[np.linalg.norm(data, axis=1) == 0] = 1.0 / np.sqrt(data.shape[1])
    data /= np.linalg.norm(data, axis=1)[:, np.newaxis]
    return data


class Dataset:
    def __init__(self, logger: logging = None):
        self.d = -1
        self.metric = "L2"  # or InnerProduct
        self.nq = -1
        self.nb = -1
        self.nt = -1
        self.url = ""
        self.basedir = ""
        self.logger = logger

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

    def __init__(self, logger: logging = None):
        self.d = 128
        self.metric = "L2"
        self.nq = 100
        self.nb = 10000
        self.nt = -1
        self.url = "ftp://ftp.irisa.fr"
        self.basedir = "datasets/"
        self.logger = logger

        self.download()

    def download(self):
        dirname = "local/texmex/corpus/"
        filename = "siftsmall.tar.gz"
        host = get_ftp_ip(self.url)
        if (
            download_from_irisa(self.logger, host, dirname, self.basedir, filename)
            == False
        ):
            return
        untar(self.logger, filename, self.basedir, "siftsmall")

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

    def __init__(self, logger: logging = None):
        self.d = 128
        self.metric = "L2"
        self.nq = 10000
        self.nb = 10**6
        self.nt = -1
        self.url = "ftp://ftp.irisa.fr"
        self.basedir = "datasets/"
        self.logger = logger

        self.download()

    def download(self):
        dirname = "local/texmex/corpus/"
        filename = "sift.tar.gz"
        host = get_ftp_ip(self.url)
        if (
            download_from_irisa(self.logger, host, dirname, self.basedir, filename)
            == False
        ):
            return
        untar(self.logger, filename, self.basedir, "sift")

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

    def __init__(self, logger: logging = None):
        import h5py

        self.metric = "InnerProduct"
        self.d, self.nt = 100, 0

        self.url = "http://ann-benchmarks.com/glove-100-angular.hdf5"
        self.basedir = "datasets/glove/"
        self.logger = logger
        self.download()

        self.glove_h5py = h5py.File(self.basedir + "glove-100-angular.hdf5", "r")
        self.nb = self.glove_h5py["train"].shape[0]
        self.nq = self.glove_h5py["test"].shape[0]

    def download(self):
        import requests

        fname = self.basedir + "glove-100-angular.hdf5"
        if os.path.isfile(fname):
            self.logger.debug("%s exists, no need to download" % (fname))
            return
        if not os.path.exists(self.basedir):
            os.makedirs(self.basedir)
        response = requests.get(self.url)
        if response.status_code == 200:
            with open(fname, "wb") as file:
                file.write(response.content)
        else:
            self.logger.error(
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

    def __init__(self, logger: logging = None):
        import h5py

        self.metric = "InnerProduct"
        self.d, self.nt = 100, 0

        self.url = "http://ann-benchmarks.com/nytimes-256-angular.hdf5"
        self.basedir = "datasets/nytimes/"
        self.logger = logger
        self.download()

        self.nytimes_h5py = h5py.File(self.basedir + "nytimes-256-angular.hdf5", "r")
        self.nb = self.nytimes_h5py["train"].shape[0]
        self.nq = self.nytimes_h5py["test"].shape[0]

    def download(self):
        import requests

        fname = self.basedir + "nytimes-256-angular.hdf5"
        if os.path.isfile(fname):
            self.logger.debug("%s exists, no need to download" % (fname))
            return
        if not os.path.exists(self.basedir):
            os.makedirs(self.basedir)
        response = requests.get(self.url)
        if response.status_code == 200:
            with open(fname, "wb") as file:
                file.write(response.content)
        else:
            self.logger.error(
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
        self.metric = "InnerProduct"
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

    def __init__(self, logger: logging = None):
        self.d = 960
        self.metric = "L2"
        self.nq = 1000
        self.nb = 10**6
        self.nt = -1
        self.url = "ftp://ftp.irisa.fr"
        self.basedir = "datasets/"
        self.logger = logger

        self.download()

    def download(self):
        dirname = "local/texmex/corpus/"
        filename = "gist.tar.gz"
        host = get_ftp_ip(self.url)
        if (
            download_from_irisa(self.logger, host, dirname, self.basedir, filename)
            == False
        ):
            return
        untar(self.logger, filename, self.basedir, "gist")

    def get_database(self):
        return fvecs_read(self.basedir + "gist/gist_base.fvecs")

    def get_queries(self):
        return fvecs_read(self.basedir + "gist/gist_query.fvecs")

    def get_groundtruth(self):
        return ivecs_read(self.basedir + "gist/gist_groundtruth.ivecs")


class DatasetRandom(Dataset):
    """
    Data from random
    """

    def __init__(self, logger: logging = None, args: argparse.Namespace = None):
        self.d = args.dimension
        self.metric = "L2"
        self.nq = args.nq
        self.nb = args.nb
        self.nt = -1
        self.k = args.limit
        self.url = ""
        self.basedir = "datasets/"
        self.logger = logger

        self.download()

    def download(self):
        return

    def get_database(self):
        return np.random.rand(self.nb, self.d)

    def get_queries(self):
        return np.random.rand(self.nq, self.d)

    def get_groundtruth(self):
        return np.random.rand(self.nq, self.k)


def get_dataset_by_name(logger: logging, args: argparse.Namespace):
    dataset = None
    if args.dataset == "sift":
        dataset = DatasetSift1M(logger)
    elif args.dataset == "siftsmall":
        dataset = DatasetSift10K(logger)
    elif args.dataset == "glove":
        dataset = DatasetGlove(logger)
    elif args.dataset == "nytimes":
        dataset = DatasetNytimes(logger)
    elif args.dataset == "gist":
        dataset = DatasetGist1M(logger)
    elif args.dataset == "random":
        dataset = DatasetRandom(logger, args)
        args.recall = False
    else:
        raise Exception("Not supported dataset")

    # reset
    args.nb, args.dimension = dataset.get_database().shape
    ncentroids = int(4 * math.sqrt(args.nb))
    if ncentroids * 39 > args.nb:
        if args.nb <= 10000:
            ncentroids = int(args.nb / 39 / args.partition_num / 2)
        else:
            ncentroids = int(args.nb / 39 / args.partition_num)

    args.nq = dataset.get_queries().shape[0]
    params = {
        "metric_type": dataset.metric,
        "ncentroids": ncentroids,
        "nsubvector": int(args.dimension / 4),
        "nlinks": 32,
        "efConstruction": 80,
    }
    # TODO data type

    if len(args.index_params) == 0:
        args.index_params = params
    return dataset.get_database(), dataset.get_queries(), dataset.get_groundtruth()


def load_config(config_file: str) -> Dict[str, Any]:
    configs = {}
    with open(config_file, "r") as stream:
        try:
            configs = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            print(f"Error loading YAML from {config_file}: {e}")
    return configs
