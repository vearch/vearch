from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from vearch.utils import IndexType

logger = logging.getLogger("vearch")


# class IndexParams(NamedTuple):
#     metric_type: str = MetricType.Inner_product
#     training_threshold: int = 100000
#     ncentroids: int = 2048
#     nsubvector: int = 64
#     bucket_init_size: int = 1000
#     buckert_max_size: int = 1280000
#     nlinks: int = 32
#     efConstruction: int = 40


class Index:
    def __init__(
        self, index_name: str, index_type: str, params: Optional[Dict[str, Any]] = None
    ):
        self._index_name = index_name
        self._index_type = index_type
        self._params = params

    def __dict__(self):
        d = {
            "name": self._index_name,
            "type": self._index_type,
        }
        if self._params is not None:
            d["params"] = self._params
        return d

    def dict(self):
        return self.__dict__()

    @classmethod
    def from_dict(cls, index_data: Dict) -> Index:
        return cls(
            index_data["name"],
            index_data["type"],
            index_data.get("params", None),
        )


class ScalarIndex(Index):
    def __init__(self, index_name: str):
        super().__init__(index_name, IndexType.SCALAR)


class IvfPQIndex(Index):
    def __init__(
        self,
        index_name: str,
        metric_type: str,
        ncentroids: int,
        nsubvector: int,
        training_threshold: Optional[int] = None,
        bucket_init_size: int = 1000,
        bucket_max_size: int = 1280000,
    ):
        params = {
            "metric_type": metric_type,
            "ncentroids": ncentroids,
            "nsubvector": nsubvector,
            "bucket_init_size": bucket_init_size,
            "bucket_max_size": bucket_max_size,
            "training_threshold": training_threshold
            if training_threshold
            else int(ncentroids * 39),
        }
        super().__init__(index_name, IndexType.IVFPQ, params)

    def nsubvector(self):
        return self._params["nsubvector"]


class IvfFlatIndex(Index):
    def __init__(
        self,
        index_name: str,
        metric_type: str,
        ncentroids: int,
        training_threshold: int = None,
    ):
        params = {
            "metric_type": metric_type,
            "ncentroids": ncentroids,
            "training_threshold": training_threshold
            if training_threshold
            else int(ncentroids * 39),
        }
        super().__init__(index_name, IndexType.IVFFLAT, params)


class BinaryIvfIndex(Index):
    """
    check vector length is powder of 8
    """

    def __init__(self, index_name: str, ncentroids: int):
        params = {
            "ncentroids": ncentroids,
        }
        super().__init__(index_name, IndexType.BINARYIVF, params)


class FlatIndex(Index):
    def __init__(self, index_name: str, metric_type: str):
        super().__init__(index_name, IndexType.FLAT, {"metric_type": metric_type})


class HNSWIndex(Index):
    def __init__(
        self,
        index_name: str,
        metric_type: str,
        nlinks: int,
        efConstruction: int,
    ):
        params = dict(
            metric_type=metric_type, nlinks=nlinks, efConstruction=efConstruction
        )
        super().__init__(index_name, IndexType.HNSW, params)


class GPUIvfPQIndex(Index):
    def __init__(
        self,
        index_name: str,
        metric_type: str,
        ncentroids: int,
        nsubvector: int,
    ):
        params = dict(
            metric_type=metric_type, ncentroids=ncentroids, nsubvector=nsubvector
        )
        super().__init__(index_name, IndexType.GPU_IVFPQ, params)
