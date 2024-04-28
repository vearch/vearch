from enum import IntEnum
import re
import logging
import logging.handlers
import logging.config
import datetime
import base64
from requests.auth import HTTPBasicAuth

LOG_LEVEL = "DEBUG"

LOGGING_CONF = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "err_formatter_type": {
            "format": "%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s",
        },
        "normal": {
            "format": "%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s",
        }

    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": LOG_LEVEL,
            "formatter": "normal",
        },
        "vearch_sdk_err": {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "filename": "err.log",
            "when": "midnight",
            "interval": 1,
            "backupCount": 7,
            "level": "ERROR",
            "formatter": "err_formatter_type",
        },
    },
    "loggers": {
        "vearch": {
            "handlers": ["console", "vearch_sdk_err"],
            "level": LOG_LEVEL,
        },
    },
}

logging.config.dictConfig(LOGGING_CONF)


def singleton(cls):
    _instance = {}

    def inner():
        if cls not in _instance:
            _instance[cls] = cls()
        return _instance[cls]

    return inner


class CodeType(IntEnum):
    CREATE_DATABASE = 2019
    LIST_DATABASES = 2020
    DROP_DATABASE = 2021
    GET_DATABASE = 2022
    CHECK_DATABASE_EXIST = 2023
    CREATE_SPACE = 2024
    CHECK_SPACE_EXIST = 2025
    LIST_SPACES = 2026
    DROP_SPACE = 2027
    CREARE_INDEX = 2028
    QUERY_DOC = 2029
    SEARCH_DOC = 2030
    UPSERT_DOC = 2031
    DELETE_DOC = 2032


class DataType:
    NONE = "none"
    INTEGER = "integer"
    LONG = "long"
    FLOAT = "float"
    DOUBLE = "double"
    STRING = "string"
    STRING_ARRAY = "string_array"
    VECTOR = "vector"
    UNKNOWN = "unknown"


class MetricType:
    Inner_product = "InnerProduct"
    L2 = "L2"


"""

index type

"SCALAR_INDEX","IVFPQ", "IVFFLAT",  "BINARYIVF", "FLAT", "HNSW", "GPU", "SSG", "IVFPQ_RELAYOUT", "SCANN"
"""


class IndexType:
    NONE = "NONE"
    SCALAR = "SCALAR"
    IVFPQ = "IVFPQ"
    IVFFLAT = "IVFFLAT"
    BINARYIVF = "BINARYIVF"
    FLAT = "FLAT"
    HNSW = "HNSW"
    GPU_IVFPQ = "GPU_IVFPQ"
    SSG = "SSG"
    IVFPQ_RELAYOUT = "IVFPQ_RELATOUT"
    SCANN = "SCANN"
    UNKNOWN = "UNKNOWN"


class VectorInfo:

    def __init__(self, field_name, feature, min_score=-1, max_score=-1, weight=-1):
        self.field_name = field_name
        self.feature = feature
        self.min_score = min_score if min_score != -1 else -1
        self.max_score = max_score if max_score != -1 else -1
        self.weight = weight if weight != -1 else -1

    def dict(self):
        vi_dict = {"field": self.field_name, "feature": self.feature}
        if self.min_score != -1:
            vi_dict["min_score"] = self.min_score
        if self.max_score != -1:
            vi_dict["max_score"] = self.max_score
        if self.weight != -1:
            vi_dict["weight"] = self.weight
        return vi_dict


reg_exp = "^([a-zA-Z]+)([a-z0-9A-Z]*[\-\_]{0,1}[a-z0-9A-Z]+)+"


def name_valid_check(name: str) -> bool:
    pattern = re.compile(reg_exp)
    match_ret = pattern.match(name)
    return match_ret.span()[1] - match_ret.span()[0] == len(name)


def compute_sign_auth(user="root", secret="secret"):
    sign = HTTPBasicAuth(user, secret)

    # sign = base64.b64encode(bytes(user + ":" + secret, encoding="utf-8"))
    return sign


if __name__ == "__main__":
    print(name_valid_check("fdafjdsj_fdasjf"))
