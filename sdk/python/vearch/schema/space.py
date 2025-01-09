import logging
from typing import List

from vearch.schema.field import Field
from vearch.schema.index import BinaryIvfIndex, IvfPQIndex
from vearch.utils import DataType

logger = logging.getLogger("vearch")


class SpaceSchema:
    def __init__(
        self,
        name: str,
        fields: List[Field],
        description: str = "",
        partition_num: int = 1,
        replica_num: int = 3,
    ):
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

    def __dict__(self):
        space_schema = {
            "name": self.name,
            "desc": self.description,
            "partition_num": self.partition_num,
            "replica_num": self.replica_num,
        }

        space_schema["fields"] = [field.__dict__() for field in self.fields]
        return space_schema

    def dict(self):
        return self.__dict__()

    @classmethod
    def from_dict(cls, data_dict):
        name = data_dict.get("space_name")
        schema_dict = data_dict.get("schema")
        fields = [Field.from_dict(field) for field in schema_dict.get("fields")]
        return cls(
            name=name,
            fields=fields,
            description=data_dict.get("desc", ""),
            partition_num=data_dict.get("partition_num"),
            replica_num=data_dict.get("replica_num"),
        )
