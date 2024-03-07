from vearch.core.space import Space
from typing import List
from vearch.core.client import client
from vearch.core.result import Result, ResultStatus, get_result
from vearch.core.const import DATABASE_URI, SPACE_URI
from vearch.schema.space import SpaceSchema
import requests


class Database(object):
    def __init__(self, name):
        self.name = name
        self.client = client

    def exist(self) -> bool:
        url_params = {"database": self.name}
        url = self.client.host + DATABASE_URI % url_params
        req = requests.request(method="GET", url=url, headers={"token": self.client.token})
        resp = self.client.s.send(req)
        result = get_result(resp)
        if result.code == ResultStatus.success:
            return result.content == self.name
        else:
            raise Exception("get database info error")
        return False

    def create(self) -> Result:
        self.client._create_db(self.name)

    def drop(self) -> Result:
        if self.exist():
            return self.client._drop_db(self.name)
        return Result(code=ResultStatus.success)

    def list_spaces(self) -> List[Space]:
        return [Space("fdasfs", "fdsafsd"), ]

    def space(self, name) -> Space:
        if not self.exist():
            result = self.create()
            if result.code == ResultStatus.success:
                return Space(self.name, name)
            else:
                raise Exception("database not exist, and create error")
        return Space(self.name, name)

    def create_space(self, space: SpaceSchema) -> Result:
        if not self.exist():
            result = self.create()
            if result.code != ResultStatus.success:
                raise Exception("database not exist,and create error")
        url_params = {"database_name": self.name, "space_name": space._name}
        url = self.client.host + SPACE_URI % url_params
        req = requests.request(method="POST", url=url, data=space.dict(), headers={"token": self.client.token})
        resp = self.client.s.send(req)
        return get_result(resp)
