import logging
from typing import List

from vearch.const import (
    CODE_DATABASE_NOT_EXIST,
    CODE_SUCCESS,
    MSG_NOT_EXIST,
)
from vearch.core.client import RestClient
from vearch.core.space import Space
from vearch.exception import SpaceException, VearchException
from vearch.result import Result
from vearch.schema.space import SpaceSchema
from vearch.utils import CodeType

logger = logging.getLogger("vearch")


class Database(object):
    def __init__(self, name: str, client: RestClient):
        self.name = name
        self.client = client

    def exist(self) -> bool:
        try:
            result = self.client._get_db_detail(self.name)
            if result.is_success():
                return True
            else:
                return False
        except VearchException as e:
            if e._code == CODE_DATABASE_NOT_EXIST and MSG_NOT_EXIST in e._msg:
                return False

    def create(self) -> Result:
        return self.client._create_db(self.name)

    def drop(self) -> Result:
        if self.exist():
            return self.client._drop_db(self.name)
        return Result(code=CODE_SUCCESS)

    def list_spaces(self) -> List[Space]:
        result = self.client._list_space(self.name)
        spaces = []
        if result.is_success():
            space_datas = result.data
            for space_data in space_datas:
                space = Space(self.name, space_data["space_name"], self.client)
                spaces.append(space)
            return spaces
        elif MSG_NOT_EXIST in result.msg:
            return spaces
        else:
            raise SpaceException(
                code=CodeType.LIST_SPACES, message="list space failed:" + result.msg
            )

    def space(self, space_name: str) -> Space:
        if not self.exist():
            result = self.create()
            if result.is_success():
                return Space(self.name, space_name, self.client)
            else:
                raise Exception("database not exist, and create error")
        return Space(self.name, space_name, self.client)

    def create_space(self, space: SpaceSchema) -> Result:
        return self.space(space.name).create(space)
