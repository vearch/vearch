from enum import IntEnum


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
    inner_product = "InnerProduct"
    L2 = "L2"


"""

index type

"SCALAR_INDEX","IVFPQ", "IVFFLAT",  "BINARYIVF", "FLAT", "HNSW", "GPU", "SSG", "IVFPQ_RELAYOUT", "SCANN"
"""


class IndexType:
    SCALAR = 0
    IVFPQ = 1
    IVFFLAT = 2
    BINARYIVF = 3
    FLAT = 4
    HNSW = 5
    GPU_IVFPQ = 6
    SSG = 7
    IVFPQ_RELAYOUT = 8
    SCANN = 9
