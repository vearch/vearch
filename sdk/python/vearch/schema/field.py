from vearch.utils import DataType
from typing import Optional
from vearch.schema.index import Index
import copy


class Field:
    def __init__(self, name: str, data_type: DataType, index: Optional[Index] = False, desc: str = "", **kwargs):
        self.name = name
        self.data_type = data_type
        self.desc = desc
        self.index = index
        self._kwargs = copy.deepcopy(kwargs)

    def dict(self):
        field_dict = {"field_name": self.name, "data_type": self.data_type, "desc": self.desc}
        if self.index:
            if  isinstance(self.index, Index):
                field_dict["index"] = self.index.dict()
            else:
                field_dict["index"] = True
        else:
            field_dict["index"] = False
        return field_dict
