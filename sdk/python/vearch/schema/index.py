import copy

import random
from typing import Optional
from vearch.utils import IndexType


class Index:
    """
        "engine": {
            "index_size": 70000,
            "id_type": "String",
            "retrieval_type": "IVFPQ",
            "retrieval_param": {
                "metric_type": "InnerProduct",
                "ncentroids": 2048,
                "nsubvector": 32
            }
        }
    """

    def __init__(self, index_name: str):
        self._index_name = index_name if index_name else ""

    def dict(self):
        return {"index_name": self._index_name, "field_name": self._field_name}


class ScalarIndex(Index):
    def __init__(self, index_name: str):
        super().__init__(index_name)


class IvfPQIndex(Index):
    def __init__(self, index_name: str, create_index_threshold: int, metric_type: str, ncentroids: int,
                 nsubvector: int, bucket_init_size: Optional[int] = None, bucket_max_size: Optional[int] = None):
        super().__init__(index_name)
        self._create_index_threshold = create_index_threshold
        self._metric_type = metric_type
        self._ncentroids = ncentroids
        self._nsubvector = nsubvector
        self._bucket_init_size = bucket_init_size if bucket_init_size else 1000
        self._bucket_max_size = bucket_max_size if bucket_max_size else 1280000

    def dict(self):
        return {"index_name": self._index_name, "index_type": IndexType.IVFPQ,
                "create_index_threshold": self._create_index_threshold,
                "metric_type": self._metric_type, "ncentroids": self._ncentroids, "nsubvector": self._nsubvector,
                "bucket_init_size": self._bucket_init_size, "bucket_max_size": self._bucket_max_size}


class IvfFlatIndex(Index):
    def __init__(self, index_name: str, metric_type: str, ncentroids: int):
        super().__init__(index_name)
        self._metric_type = metric_type
        self._ncentroids = ncentroids

    def dict(self):
        return {"index_name": self._index_name, "index_type": IndexType.IVFFLAT, "metric_type": self._metric_type,
                "ncentroids": self._ncentroids}


class BinaryIvfIndex(Index):
    """
    check vector length is powder of 8
    """

    def __init__(self, index_name: str, ncentroids: int):
        super().__init__(index_name)
        self._ncentroids = ncentroids

    def dict(self):
        return {"index_name": self._index_name, "index_type": IndexType.BINARYIVF, "ncentroids": self._ncentroids}


class FlatIndex(Index):
    def __init__(self, index_name: str, metric_type: str):
        super().__init__(index_name)
        self._metric_type = metric_type

    def dict(self):
        return {"index_name": self._index_name, "index_type": IndexType.FLAT, "metric_type": self._metric_type}


class HNSWIndex(Index):
    def __init__(self, index_name: str, metric_type: str, nlinks: int, efConstruction: int):
        super().__init__(index_name)
        self._metric_type = metric_type
        self._nlinks = nlinks
        self._efConstruction = efConstruction

    def dict(self):
        return {"index_name": self._index_name, "index_type": IndexType.HNSW, "nlinks": self._nlinks,
                "efConstruction": self._efConstruction}


class GPUIvfPQIndex(Index):
    def __int__(self, index_name: str, metric_type: str, ncentroids: int, nsubvector: str):
        super().__init__(index_name)
        self._metric_type = metric_type
        self._ncentroids = ncentroids
        self._nsubvector = nsubvector

    def dict(self):
        return {"index_name": self._index_name, "index_type": IndexType.GPU_IVFPQ,
                "metric_type": self._metric_type,
                "ncentroids": self._ncentroids, "nsubvector": self._nsubvector}
