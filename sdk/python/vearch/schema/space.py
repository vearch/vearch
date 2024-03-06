from typing import List, Optional
from vearch.utils import DataType
from vearch.schema.index import Index, BinaryIvfIndex


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
            if field.index and field.index._index_type>1:
                assert field.data_type not in [DataType.NONE, DataType.UNKNOWN]
                if isinstance(field.index, BinaryIvfIndex):
                    assert field.dim // 8 == 0, "BinaryIvfIndex vector dimention must be power of eight"

    def dict(self):
        space_schema = {"name": self._name, "desc": self._description, "partition_num": self._partition_num,
                        "replication_num": self._replication_num}
        fields_dict = [field.dict() for field in self._fields]
        space_schema["fields"] = fields_dict
        return space_schema
