from typing import List, Optional
from vearch.utils import DataType
from vearch.schema.index import BinaryIvfIndex
from vearch.schema.field import Field
import logging
import json

logger = logging.getLogger("vearch")


class SpaceSchema:
    def __init__(self, name, fields: List, description: str = "",
                 partition_num: int = 1,
                 replication_num: int = 3):
        """

        :param fields:
        :param description:
        field=Field("field1",DataType.INT64,"record count")
        SpaceSchema(fields=[field,],description="the description of the space")
        """
        self._name = name
        self._fields = fields
        self._description = description
        self._partition_num = partition_num
        self._replication_num = replication_num
        self._check_valid()

    def _check_valid(self):
        for field in self._fields:
            if field.index:
                assert field.data_type not in [DataType.NONE, DataType.UNKNOWN]
                if isinstance(field.index, BinaryIvfIndex):
                    assert field.dim // 8 == 0, "BinaryIvfIndex vector dimention must be power of eight"

    @property
    def name(self):
        return self._name

    def dict(self):
        space_schema = {"name": self._name, "desc": self._description, "partition_num": self._partition_num,
                        "replication_num": self._replication_num}

        logger.debug("space_schema" + json.dumps(space_schema))

        fields_dict = [field.dict() for field in self._fields]
        space_schema["fields"] = fields_dict
        return space_schema

    @classmethod
    def from_dict(cls, data_dict):
        fields = [Field.from_dict(field) for field in data_dict.get("fields")]
        space_schema = SpaceSchema(name=data_dict.get("name"), fields=fields, description=data_dict.get("desc"),
                                   partition_num=data_dict.get("partition_num"),
                                   replication_num=data_dict.get("replication_num"))
        return space_schema
