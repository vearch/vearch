from vearch.schema.index import Index
from vearch.core.client import client
from vearch.schema.space import SpaceSchema
from vearch.core.result import Result, ResultStatus, get_result
from vearch.core.const import SPACE_URI
import requests


class Space(object):
    def __int__(self, db_name, name):
        self.db_name = db_name
        self.name = name
        self.client = client

    def create(self, space: SpaceSchema) -> Result:
        url_params = {"database_name": self.db_name, "space_name": space._name}
        url = self.client.host + SPACE_URI % url_params
        req = requests.request(method="POST", url=url, data=space.dict(), headers={"token": self.client.token})
        resp = self.client.s.send(req)
        return get_result(resp)

    def drop(self) -> Result:
        url_params = {"database_name": self.name, "space_name": self.name}
        url = self.client.host + SPACE_URI % url_params
        req = requests.request(method="POST", url=url, headers={"token": self.client.token})
        resp = self.client.s.send(req)
        return get_result(resp)

    def exist(self) -> bool:
        pass
        return True

    def create_index(self, field: str, index: Index):
        pass
