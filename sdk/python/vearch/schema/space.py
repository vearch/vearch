from typing import List, Optional
from vearch.utils import DataType
from vearch.schema.index import BinaryIvfIndex, IvfPQIndex
from vearch.schema.field import Field
import logging
import json

logger = logging.getLogger("vearch")


class SpaceSchema:
    def __init__(self, name: str, fields: List, description: str = "",
                 partition_num: int = 1,
                 replication_num: int = 3):
        """

        :param name:
        :param fields:
        :param description:
        field=Field("field1",DataType.INT64,"record count")
        SpaceSchema(fields=[field,],description="the description of the space")
        """
        self.name = name
        self.fields = fields
        self.description = description
        self.partition_num = partition_num
        self.replication_num = replication_num
        self._check_valid()

    def _check_valid(self):
        for field in self.fields:
            if field.index:
                assert field.data_type not in [DataType.NONE, DataType.UNKNOWN]
                if isinstance(field.index, BinaryIvfIndex):
                    assert field.dim % 8 == 0, "BinaryIvfIndex vector dimention must be power of eight"
                if isinstance(field.index, IvfPQIndex):
                    assert field.dim % field.index.nsubvector() == 0, "IVFPQIndex vector dimention must be power of nsubvector"

    def dict(self):
        space_schema = {"name": self.name, "desc": self.description, "partition_num": self.partition_num,
                        "replication_num": self.replication_num}

        fields_dict = [field.dict() for field in self.fields]
        space_schema["fields"] = fields_dict
        return space_schema

    @classmethod
    def from_dict(cls, data_dict):
     
        name = data_dict.get("space_name")
        schema_dict = data_dict.get("schema")
        fields = [Field.from_dict(field) for field in schema_dict.get("fields")]
        return cls(name=name, fields=fields,
                   description=data_dict.get("desc", ""),
                   partition_num=data_dict.get("partition_num"),
                   replication_num=data_dict.get("replica_num"))
