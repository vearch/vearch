import copy

import random
from typing import Optional
from vearch.utils import IndexType, MetricType

from typing import NamedTuple
import logging

logger = logging.getLogger("vearch")


class Index:
    def __init__(self, index_name: str, index_type: str, **kwargs):
        self._index_name = index_name if index_name else ""
        self._index_type = index_type
        self._kwargs = copy.deepcopy(kwargs)

    def dict(self):
        return {"name": self._index_name}


class IndexParams(NamedTuple):
    metric_type: str = MetricType.Inner_product
    training_threshold: int = 100000
    ncentroids: int = 2048
    nsubvector: int = 64
    bucket_init_size: int = 1000
    buckert_max_size: int = 1280000
    nlinks: int = 32
    efConstruction: int = 40


class ScalarIndex(Index):
    def __init__(self, index_name: str, **kwargs):
        super().__init__(index_name, IndexType.SCALAR, **kwargs)

    def dict(self):
        return {"name": self._index_name, "type": IndexType.SCALAR}


class IvfPQIndex(Index):
    def __init__(self, index_name: str, training_threshold: int, metric_type: str, ncentroids: int,
                 nsubvector: int, bucket_init_size: Optional[int] = None, bucket_max_size: Optional[int] = None,
                 **kwargs):
        super().__init__(index_name, index_type=IndexType.IVFPQ, **kwargs)
        self._index_params = IndexParams(training_threshold=training_threshold, metric_type=metric_type,
                                         ncentroids=ncentroids, nsubvector=nsubvector)
        self._index_params._replace(
            bucket_init_size=bucket_init_size if bucket_init_size else 1000)
        self._index_params._replace(
            buckert_max_size=bucket_max_size if bucket_max_size else 1280000)

    def dict(self):
        return {"name": self._index_name, "type": IndexType.IVFPQ,
                "params": {
                    "training_threshold": self._index_params.training_threshold,
                    "metric_type": self._index_params.metric_type, "ncentroids": self._index_params.ncentroids,
                    "nsubvector": self._index_params.nsubvector,
                    "bucket_init_size": self._index_params.bucket_init_size,
                    "bucket_max_size": self._index_params.buckert_max_size
                }
                }

    def nsubvector(self):
        return self._index_params.nsubvector


class IvfFlatIndex(Index):
    def __init__(self, index_name: str, metric_type: str, ncentroids: int, **kwargs):
        super().__init__(index_name, index_type=IndexType.IVFFLAT, **kwargs)
        self._index_params = IndexParams(
            metric_type=metric_type, ncentroids=ncentroids)

    def dict(self):
        return {"name": self._index_name, "type": IndexType.IVFFLAT,
                "params": {
                    "metric_type": self._index_params.metric_type,
                    "ncentroids": self._index_params.ncentroids
                }
                }


class BinaryIvfIndex(Index):
    """
    check vector length is powder of 8
    """

    def __init__(self, index_name: str, ncentroids: int, **kwargs):
        super().__init__(index_name, index_type=IndexType.BINARYIVF, **kwargs)
        self._index_params = IndexParams(ncentroids=ncentroids)

    def dict(self):
        return {"name": self._index_name, "type": IndexType.BINARYIVF,
                "params": {
                    "ncentroids": self._index_params.ncentroids}
                }


class FlatIndex(Index):
    def __init__(self, index_name: str, metric_type: str, **kwargs):
        super().__init__(index_name, index_type=IndexType.FLAT, **kwargs)
        self._index_params = IndexParams(metric_type=metric_type)

    def dict(self):
        return {"name": self._index_name, "type": IndexType.FLAT,
                "params": {
                    "metric_type": self._index_params.metric_type
                }
                }


class HNSWIndex(Index):
    def __init__(self, index_name: str, metric_type: str, nlinks: int, efConstruction: int, **kwargs):
        super().__init__(index_name, index_type=IndexType.HNSW, **kwargs)
        self._index_params = IndexParams(
            metric_type=metric_type, nlinks=nlinks, efConstruction=efConstruction)

    def dict(self):
        return {"name": self._index_name, "type": IndexType.HNSW, "params": {
            "nlinks": self._index_params.nlinks,
            "efConstruction": self._index_params.efConstruction, "metric_type": self._index_params.metric_type
        }
        }


class GPUIvfPQIndex(Index):
    def __init__(self, index_name: str, metric_type: str, ncentroids: int, nsubvector: int, **kwargs):
        super().__init__(index_name, index_type=IndexType.GPU_IVFPQ, **kwargs)
        self._index_params = IndexParams(
            metric_type=metric_type, ncentroids=ncentroids, nsubvector=nsubvector)

    def dict(self):
        return {"name": self._index_name, "type": IndexType.GPU_IVFPQ,
                "params": {
                    "metric_type": self._index_params.metric_type,
                    "ncentroids": self._index_params.ncentroids, "nsubvector": self._index_params.nsubvector
                }
                }
