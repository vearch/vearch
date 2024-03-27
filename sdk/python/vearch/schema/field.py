from vearch.utils import DataType
from typing import Optional
from vearch.schema.index import Index
from vearch.utils import name_valid_check
import copy
import logging
import json

logger = logging.getLogger("vearch")


class Field:
    def __init__(self, name:str, data_type: DataType, index: Optional[Index] = None, desc: str = "", **kwargs):
        """

        :param name: the name of field
        :param data_type: the data type of field,if data_type set DataType.VARCHAR the property array can set True to support multi values;
        if data_type set DataType.VECTOR the property dim is must be setted
        :param index:
        :param desc:
        :param kwargs:
        """
        self.name = name
        self.data_type = data_type
        self.desc = desc
        self.index = index if index else None
        self._kwargs = copy.deepcopy(kwargs)
        self._valid_check()

    def _valid_check(self):
        if self.data_type == DataType.VECTOR:
            logger.debug(self.data_type)
            self.dim = self._kwargs.get("dimension", None)
            logger.debug(self.dim)
            assert isinstance(self.dim, int), "vector field must set dimention,you should set dim=xxx"
            assert self.dim > 0, "the vector field's dimention must above zero"
        if self.data_type == DataType.STRING:
            self.array = self._kwargs.get("array", False)
        assert name_valid_check(
            self.name) == True, "field name must match ^([a-zA-Z]+)([a-z0-9A-Z]*[\-\_]{0,1}[a-z0-9A-Z]+)+"

    def dict(self):
        field_dict = {"name": self.name, "type": self.data_type, "desc": self.desc}
        logger.debug(json.dumps(field_dict))
        if self.data_type == DataType.VECTOR:
            field_dict["dimension"] = self.dim
        if self.index:
            field_dict["index"] = self.index.dict()
        logger.debug(json.dumps(field_dict))
        return field_dict

    @classmethod
    def from_dict(cls, field_data):
        name = field_data.pop("name")
        data_type = field_data.pop("type")
        describe = field_data.pop("desc")
        index = field_data.pop("index", None)
        logger.debug(field_data)
        if index:
            return Field(name, data_type=data_type, index=index, desc=describe, **field_data)
        return Field(name=name, data_type=data_type, desc=describe, **field_data)
