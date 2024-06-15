import argparse
import psutil
import os
import logging


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
        "--batch",
        help="the batch size",
        type=int,
        default=100,
    )
    parser.add_argument(
        "--partition",
        help="the partition num",
        type=int,
        default=3,
    )
    parser.add_argument(
        "--replica",
        help="the replica num",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--pool",
        help="the thread pool size",
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
    parser.add_argument(
        "--index",
        default="FLAT",
        type=str,
        help="the vector index type",
    )
    parser.add_argument(
        "--verbose",
        default=True,
        type=bool,
        help="show detail infomation",
    )
    parser.add_argument(
        "--dataset",
        default="Random",
        type=str,
        help="the dataset to load",
    )
    parser.add_argument(
        "--limit",
        default=100,
        type=int,
        help="the number of near neighbours to search for",
    )
    parser.add_argument(
        "--nb",
        help="the total num of embedding",
        type=int,
        default=10000,
    )
    parser.add_argument(
        "--nq",
        default=10000,
        type=int,
        help="the number of interface requests of [query, delete, search]",
    )
    parser.add_argument(
        "--log",
        default="INFO",
        type=str,
        help="the log level",
        choices=log_levels.keys(),
    )
    args = parser.parse_args()

    return args
