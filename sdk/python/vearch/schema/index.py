from __future__ import annotations

import logging
import warnings
from typing import Any, Dict, Optional, List, Union

from vearch.utils import IndexType, MetricType

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
        self, index_name: str, index_type: str, params: Optional[Dict[str, Any]] = None,
        field_name: Optional[str] = None, field_names: Optional[List[str]] = None
    ):
        self._index_name = index_name
        self._index_type = index_type
        self._params = params
        self._field_name = field_name
        self._field_names = field_names

    def to_dict(self):
        d: Dict[str, Any] = {
            "name": self._index_name,
            "type": self._index_type,
        }
        if self._params is not None:
            d["params"] = self._params
        if self._field_name is not None:
            d["field_name"] = self._field_name
        if self._field_names is not None:
            d["field_names"] = self._field_names
        return d

    def dict(self):
        return self.to_dict()

    @classmethod
    def from_dict(cls, index_data: Dict) -> Index:
        target_cls = _INDEX_TYPE_MAP.get(index_data["type"], Index)
        instance = object.__new__(target_cls)
        instance._index_name = index_data["name"]
        instance._index_type = index_data["type"]
        instance._params = index_data.get("params", None)
        instance._field_name = index_data.get("field_name", None)
        instance._field_names = index_data.get("field_names", None)
        return instance


class ScalarIndex(Index):
    def __init__(self, index_name: str, field_name: Optional[str] = None):
        super().__init__(index_name, IndexType.SCALAR, field_name=field_name)


class InvertedIndex(Index):
    def __init__(self, index_name: str, field_name: Optional[str] = None):
        super().__init__(index_name, IndexType.INVERTED, field_name=field_name)


class BitmapIndex(Index):
    def __init__(self, index_name: str, field_name: Optional[str] = None):
        super().__init__(index_name, IndexType.BITMAP, field_name=field_name)


class CompositeIndex(Index):
    def __init__(self, index_name: str, field_names: List[str]):
        super().__init__(index_name, IndexType.COMPOSITE, field_names=field_names)


class HNSWParams:
    """Sub-config for IVF indexes that compose HNSW (nlinks/efConstruction/efSearch only).

    Use HNSWIndex when you want a standalone HNSW index with a name and metric_type;
    use HNSWParams when nesting HNSW tuning inside IvfPQIndex/IvfFlatIndex/IvfRaBitQIndex.
    """

    def __init__(self, nlinks: int = 32, efConstruction: int = 160, efSearch: int = 64):
        self.nlinks = nlinks
        self.efConstruction = efConstruction
        self.efSearch = efSearch

    def _params_dict(self) -> Dict[str, Any]:
        return {
            "nlinks": self.nlinks,
            "efConstruction": self.efConstruction,
            "efSearch": self.efSearch,
        }


class HNSWIndex(Index):
    def __init__(
        self,
        index_name: str,
        metric_type: str,
        nlinks: int = 32,
        efConstruction: int = 160,
        efSearch: int = 64,
        field_name: Optional[str] = None
    ):
        params = dict(
            metric_type=metric_type, nlinks=nlinks, efConstruction=efConstruction, efSearch=efSearch
        )
        super().__init__(index_name, IndexType.HNSW, params, field_name)


def _hnsw_subparams(
    hnsw: Union[HNSWIndex, HNSWParams], outer_metric_type: Optional[str] = None
) -> Dict[str, Any]:
    if isinstance(hnsw, HNSWParams):
        return hnsw._params_dict()
    inner_metric_type = hnsw._params.get("metric_type")
    if (
        outer_metric_type is not None
        and inner_metric_type is not None
        and inner_metric_type != outer_metric_type
    ):
        warnings.warn(
            f"HNSW sub-config metric_type={inner_metric_type!r} ignored; "
            f"outer index metric_type={outer_metric_type!r} applies",
            stacklevel=3,
        )
    return {
        "nlinks": hnsw._params.get("nlinks", None),
        "efConstruction": hnsw._params.get("efConstruction", None),
        "efSearch": hnsw._params.get("efSearch", None),
    }


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
        nprobe: int = 80,
        hnsw: Optional[Union[HNSWIndex, HNSWParams]] = None,
        field_name: Optional[str] = None
    ):
        params = {
            "metric_type": metric_type,
            "ncentroids": ncentroids,
            "nsubvector": nsubvector,
            "bucket_init_size": bucket_init_size,
            "bucket_max_size": bucket_max_size,
            "training_threshold": training_threshold
            if training_threshold is not None
            else int(ncentroids * 200),
            "nprobe": nprobe
        }
        if hnsw is not None:
            params["hnsw"] = _hnsw_subparams(hnsw, outer_metric_type=metric_type)
        super().__init__(index_name, IndexType.IVFPQ, params, field_name)

    def nsubvector(self):
        if self._params is None:
            return None
        return self._params["nsubvector"]


class IvfFlatIndex(Index):
    def __init__(
        self,
        index_name: str,
        metric_type: str,
        ncentroids: int,
        training_threshold: Optional[int] = None,
        nprobe: int = 80,
        hnsw: Optional[Union[HNSWIndex, HNSWParams]] = None,
        field_name: Optional[str] = None
    ):
        params = {
            "metric_type": metric_type,
            "ncentroids": ncentroids,
            "training_threshold": training_threshold
            if training_threshold is not None
            else int(ncentroids * 200),
            "nprobe": nprobe
        }
        if hnsw is not None:
            params["hnsw"] = _hnsw_subparams(hnsw, outer_metric_type=metric_type)
        super().__init__(index_name, IndexType.IVFFLAT, params, field_name)


class BinaryIvfIndex(Index):
    """
    check vector length is powder of 8
    """

    def __init__(self, index_name: str, ncentroids: int, field_name: Optional[str] = None):
        params = {
            "ncentroids": ncentroids,
        }
        super().__init__(index_name, IndexType.BINARYIVF, params, field_name)


class FlatIndex(Index):
    def __init__(self, index_name: str, metric_type: str, field_name: Optional[str] = None):
        super().__init__(index_name, IndexType.FLAT, {"metric_type": metric_type}, field_name)


class GPUIvfPQIndex(Index):
    def __init__(
        self,
        index_name: str,
        metric_type: str,
        ncentroids: int,
        nsubvector: int,
        training_threshold: Optional[int] = None,
        nprobe: int = 80,
        field_name: Optional[str] = None
    ):
        params = dict(
            metric_type=metric_type,
            ncentroids=ncentroids,
            nsubvector=nsubvector,
            nprobe=nprobe,
            training_threshold=training_threshold
            if training_threshold is not None
            else int(ncentroids * 200),
        )
        super().__init__(index_name, IndexType.GPU_IVFPQ, params, field_name)


class GPUIvfFlatIndex(Index):
    def __init__(
        self,
        index_name: str,
        metric_type: str,
        ncentroids: int,
        training_threshold: Optional[int] = None,
        bucket_init_size: int = 1000,
        bucket_max_size: int = 1280000,
        nprobe: int = 80,
        field_name: Optional[str] = None
    ):
        params = {
            "metric_type": metric_type,
            "ncentroids": ncentroids,
            "bucket_init_size": bucket_init_size,
            "bucket_max_size": bucket_max_size,
            "training_threshold": training_threshold
            if training_threshold is not None
            else int(ncentroids * 200),
            "nprobe": nprobe
        }
        super().__init__(index_name, IndexType.GPU_IVFFLAT, params, field_name)


class _RaBitQBase(Index):
    """Shared base for IvfRaBitQ-family indexes — owns the nb_bits param and getter."""

    def get_nb_bits(self):
        if self._params is None:
            return None
        return self._params["nb_bits"]


class NPUIvfRaBitQIndex(_RaBitQBase):
    def __init__(
        self,
        index_name: str,
        metric_type: str = MetricType.L2,
        ncentroids: int = 4096,
        nb_bits: int = 1,
        training_threshold: Optional[int] = None,
        nprobe: int = 80,
        field_name: Optional[str] = None
    ):
        params = {
            "metric_type": metric_type,
            "ncentroids": ncentroids,
            "nb_bits": nb_bits,
            "training_threshold": training_threshold
            if training_threshold is not None
            else int(ncentroids * 200),
            "nprobe": nprobe
        }
        super().__init__(index_name, IndexType.NPU_IVFRABITQ, params, field_name)


class IvfRaBitQIndex(_RaBitQBase):
    def __init__(
        self,
        index_name: str,
        metric_type: str,
        ncentroids: int,
        nb_bits: int,
        training_threshold: Optional[int] = None,
        bucket_init_size: int = 1000,
        bucket_max_size: int = 1280000,
        nprobe: int = 80,
        hnsw: Optional[Union[HNSWIndex, HNSWParams]] = None,
        field_name: Optional[str] = None
    ):
        params = {
            "metric_type": metric_type,
            "ncentroids": ncentroids,
            "nb_bits": nb_bits,
            "bucket_init_size": bucket_init_size,
            "bucket_max_size": bucket_max_size,
            "training_threshold": training_threshold
            if training_threshold is not None
            else int(ncentroids * 200),
            "nprobe": nprobe
        }
        if hnsw is not None:
            params["hnsw"] = _hnsw_subparams(hnsw, outer_metric_type=metric_type)
        super().__init__(index_name, IndexType.IVFRABITQ, params, field_name)


_INDEX_TYPE_MAP: Dict[str, type] = {
    IndexType.SCALAR: ScalarIndex,
    IndexType.INVERTED: InvertedIndex,
    IndexType.BITMAP: BitmapIndex,
    IndexType.COMPOSITE: CompositeIndex,
    IndexType.IVFPQ: IvfPQIndex,
    IndexType.IVFFLAT: IvfFlatIndex,
    IndexType.BINARYIVF: BinaryIvfIndex,
    IndexType.FLAT: FlatIndex,
    IndexType.HNSW: HNSWIndex,
    IndexType.GPU_IVFPQ: GPUIvfPQIndex,
    IndexType.GPU_IVFFLAT: GPUIvfFlatIndex,
    IndexType.IVFRABITQ: IvfRaBitQIndex,
    IndexType.NPU_IVFRABITQ: NPUIvfRaBitQIndex,
}
