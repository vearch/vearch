from vearch.config import Config
from vearch.core.db import Database
from vearch.core.client import client
from vearch.core.result import Result, get_result, ResultStatus
from vearch.schema.space import SpaceSchema
from vearch.core.const import SPACE_URI
import requests
from typing import List
import json


class Vearch(object):
    def __init__(self, config: Config):
        self.client = client
        self.client.config(config)

    def create_database(self, database_name: str) -> Result:
        return self.client._create_db(database_name)

    def list_databases(self) -> List[Database]:
        result = self.client._list_db()
        l = []
        if result.code == "success":
            database_names = json.loads(result.content)
            for database_name in database_names:
                db = Database(database_name)
                l.append(db)
            return l
        else:
            raise Exception("list database failed")

    def database(self, database_name: str) -> Database:

        return Database(database_name)

    def drop_database(self, database_name: str) -> Result:
        self.client._drop_db(database_name)

    def create_space(self, database_name: str, space: SpaceSchema) -> Result:
        if not self.database(database_name).exist():
            ret = self.database(database_name).create()
            if ret.code == ResultStatus.failed:
                raise Exception("create database error")
        url_params = {"database_name": database_name, "space_name": space._name}
        url = self.client.host + SPACE_URI % url_params
        req = requests.request(method="POST", url=url, data=space.dict(), headers={"token": self.client.token})
        resp = self.client.s.send(req)
        result = get_result(resp)
        return result
