from vearch.schema.index import Index
from vearch.core.client import client
from vearch.schema.space import SpaceSchema
from vearch.core.result import Result, ResultStatus, get_result
from vearch.core.const import SPACE_URI, INDEX_URI
import requests
import json


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
        url_params = {"database_name": self.db_name, "space_name": self.name}
        url = self.client.host + SPACE_URI % url_params
        req = requests.request(method="POST", url=url, headers={"token": self.client.token})
        resp = self.client.s.send(req)
        return get_result(resp)

    def exist(self) -> bool:
        url_params = {"database_name": self.db_name, "space_name": self.name}
        url = self.client.host + SPACE_URI % url_params
        req = requests.request(method="GET", url=url, headers={"token": self.client.token})
        resp = self.client.s.send(req)
        result = get_result(resp)
        if result.code == ResultStatus.success:
            return True
        return False

    def create_index(self, field: str, index: Index) -> Result:
        url = self.client.host + INDEX_URI
        req_body = {"field": field, "index": index, "database": self.db_name, "space": self.name}
        req = requests.request(method="POST", url=url, data=json.dumps(req_body),
                               headers={"token": self.client.token})
        resp = self.client.s.send(req)
        return get_result(resp)

    def upsert_doc(self):
        pass

    def delete_doc(self):
        pass

    def search(self):
        pass

    def query(self):
        pass
