import logging
from typing import List, Optional

from vearch.schema.field import Field
from vearch.schema.index import (
    Index,
    BinaryIvfIndex,
    IvfPQIndex,
    CompositeIndex,
)
from vearch.utils import DataType

logger = logging.getLogger("vearch")


class SpaceSchema:
    def __init__(
        self,
        name: str,
        fields: List[Field],
        indexes: Optional[List[Index]] = None,
        description: str = "",
        partition_num: int = 1,
        replica_num: int = 3,
    ):
        """
        :param name: space name
        :param fields: list of Field. Per-field vector indexes can be attached via ``Field(..., index=...)``.
        :param indexes: top-level scalar / inverted / bitmap / composite indexes that span fields.
            Use either ``Field(..., index=...)`` or ``indexes=[Index]``.
        :param description:
        :param partition_num:
        :param replica_num:
        field=Field("field1",DataType.INT64,"record count")
        SpaceSchema(fields=[field,],description="the description of the space")
        """
        self.name = name
        self.fields = fields
        self.indexes = indexes
        self.description = description
        self.partition_num = partition_num
        self.replica_num = replica_num
        self._check_valid()

    def _check_valid(self):
        for field in self.fields:
            if field.index:
                assert field.data_type not in [DataType.NONE, DataType.UNKNOWN]
                if isinstance(field.index, BinaryIvfIndex):
                    assert (
                        field.dim % 8 == 0
                    ), "BinaryIvfIndex vector dimention must be power of eight"
                if isinstance(field.index, IvfPQIndex):
                    assert (
                        field.dim % field.index.nsubvector() == 0
                    ), "IVFPQIndex vector dimention must be power of nsubvector"
        if self.indexes:
            for idx in self.indexes:
                if isinstance(idx, CompositeIndex):
                    assert (
                        idx._field_names is not None and len(idx._field_names) >= 2
                    ), "CompositeIndex requires at least 2 field_names"
                else:
                    assert (
                        idx._field_name
                    ), f"{type(idx).__name__} requires a non-empty field_name when used as a top-level index"

    def to_dict(self):
        space_schema = {
            "name": self.name,
            "desc": self.description,
            "partition_num": self.partition_num,
            "replica_num": self.replica_num,
        }

        space_schema["fields"] = [field.dict() for field in self.fields]
        if self.indexes is not None:
            space_schema["indexes"] = [index.dict() for index in self.indexes]
        return space_schema

    def dict(self):
        return self.to_dict()

    @classmethod
    def from_dict(cls, data_dict):
        name = data_dict.get("space_name")
        schema_dict = data_dict.get("schema")
        fields = [Field.from_dict(field) for field in schema_dict.get("fields")]
        indexes = schema_dict.get("indexes", None)
        if indexes is not None:
            indexes = [Index.from_dict(index) for index in indexes]
        return cls(
            name=name,
            fields=fields,
            indexes=indexes,
            description=data_dict.get("desc", ""),
            partition_num=data_dict.get("partition_num"),
            replica_num=data_dict.get("replica_num"),
        )
