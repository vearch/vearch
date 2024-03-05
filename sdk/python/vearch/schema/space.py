from typing import List, SupportsInt
from vearch.utils import DataType


class SpaceSchema:
    def __init__(self, name, fields: List, description: str = "", partition_num: int = 1,
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

    def _check_valid(self):
        for field in self._fields:
            if field.index == True:
                assert field.data_type not in [DataType.NONE, DataType.UNKNOWN]
            if field.data_type == DataType.VARCHAR:
                pass

    def dict(self):
        space_schema = {"name": self._name, "desc": self._description, "partition_num": self._partition_num,
                        "replication_num": self._replication_num}
        fields_dict = [field.dict() for field in self._fields]
        space_schema["fields"] = fields_dict
        return space_schema
