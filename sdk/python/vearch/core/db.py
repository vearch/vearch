from vearch.core.space import Space
from typing import List
from vearch.core.client import client
from vearch.result import Result, ResultStatus, get_result
from vearch.const import DATABASE_URI, SPACE_URI, AUTH_KEY
from vearch.schema.space import SpaceSchema
from vearch.exception import DatabaseException
from vearch.utils import CodeType, compute_sign_auth
import requests
import logging

logger = logging.getLogger("vearch")


class Database(object):
    def __init__(self, name):
        self.name = name
        self.client = client

    def exist(self) -> bool:
        try:
            url_params = {"database_name": self.name}
            url = self.client.host + DATABASE_URI % url_params
            sign = compute_sign_auth(secret=self.client.token)
            resp = requests.request(method="GET", url=url, headers={AUTH_KEY: sign})
            result = get_result(resp)
            logger.debug("database exist return:"+result.dict_str())
            logger.debug(resp.status_code)
            if result.code == 200:
                return True
            else:
                return False
        except Exception as e:
            print(e)
            raise DatabaseException(code=CodeType.GET_DATABASE, message=e.__str__())

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
        req = requests.request(method="POST", url=url, data=space.dict(), headers={AUTH_KEY: self.client.token})
        resp = self.client.s.send(req)
        return get_result(resp)
