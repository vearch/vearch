from vearch.core.space import Space
from typing import List
from vearch.core.client import client
from vearch.result import Result, ResultStatus, get_result
from vearch.const import DATABASE_URI, SPACE_URI, LIST_SPACE_URI, AUTH_KEY, CODE_DATABASE_NOT_EXIST, CODE_DB_EXIST, CODE_SUCCESS, MSG_NOT_EXIST
from vearch.schema.space import SpaceSchema
from vearch.exception import DatabaseException, VearchException
from vearch.utils import CodeType, compute_sign_auth
import requests
import logging
import json

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
            resp = requests.request(method="GET", url=url, auth=sign)
            result = get_result(resp)
            if result.code == CODE_SUCCESS:
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
        return Result(code=ResultStatus.success)

    def list_spaces(self) -> List[Space]:
        url_params = {"database_name": self.name}
        url = self.client.host + (LIST_SPACE_URI % url_params)
        sign = compute_sign_auth(secret=self.client.token)
        resp = requests.request(method="GET", url=url, auth=sign)
        return get_result(resp)

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
        url_params = {"database_name": self.name, "space_name": space.name}
        url = self.client.host + LIST_SPACE_URI % url_params
        sign = compute_sign_auth(secret=self.client.token)
        resp = requests.request(method="POST", url=url,
                                data=json.dumps(space.dict()), auth=sign)
        return get_result(resp)
