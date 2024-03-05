from enum import IntEnum
import re


def singleton(cls):
    _instance = {}

    def inner():
        if cls not in _instance:
            _instance[cls] = cls()
        return _instance[cls]

    return inner


class DataType:
    NONE = 0
    INT32 = 1
    INT64 = 2
    DOUBLE = 3
    VARCHAR = 4
    VECTOR = 5
    UNKNOWN = 99


class MetricType:
    Inner_product = "InnerProduct"
    L2 = "L2"


"""

index type

"SCALAR_INDEX","IVFPQ", "IVFFLAT",  "BINARYIVF", "FLAT", "HNSW", "GPU", "SSG", "IVFPQ_RELAYOUT", "SCANN"
"""


class IndexType(IntEnum):
    NONE = 0,
    SCALAR = 1,
    IVFPQ = 2,
    IVFFLAT = 3,
    BINARYIVF = 4,
    FLAT = 5,
    HNSW = 6,
    GPU_IVFPQ = 7,
    SSG = 8,
    IVFPQ_RELAYOUT = 9,
    SCANN = 10,
    UNKNOWN = 999


reg_exp = "^([a-zA-Z]+)([a-z0-9A-Z]*[\-\_]{0,1}[a-z0-9A-Z]+)+"


def name_valid_check(name: str) -> bool:
    pattern = re.compile(reg_exp)
    match_ret = pattern.match(name)
    return match_ret.span()[1]-match_ret.span()[0] == len(name)


if __name__ == "__main__":
    print(name_valid_check("fdafjdsj_fdasjf"))
