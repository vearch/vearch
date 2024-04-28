from vearch.config import Config
from vearch.core.db import Database
from vearch.core.client import client
from vearch.result import Result, get_result, ResultStatus
from vearch.schema.index import Index
from vearch.schema.space import SpaceSchema
from vearch.const import SPACE_URI, INDEX_URI, AUTH_KEY, ERR_CODE_SPACE_NOT_EXIST
from vearch.exception import DatabaseException, VearchException, SpaceException
from vearch.utils import CodeType, compute_sign_auth
import requests
from typing import List
import json
import logging

logger = logging.getLogger("vearch")


class Vearch(object):
    def __init__(self, config: Config):
        self.client = client
        self.client.config(config)

    def create_database(self, database_name: str) -> Result:
        return self.client._create_db(database_name)

    def list_databases(self) -> List[Database]:
        result = self.client._list_db()
        l = []
        logger.debug(result.dict_str())
        if result.code == 0:
            logger.debug(result.text)
            database_names = result.text
            for database_name in database_names:
                db = Database(database_name)
                l.append(db)
            return l
        else:
            logger.error(result.dict_str())
            raise DatabaseException(code=CodeType.LIST_DATABASES, message="list database failed:" + result.err_msg)

    def is_database_exist(self, database_name: str) -> bool:
        db = Database(database_name)
        return db.exist()

    def database(self, database_name: str) -> Database:

        return Database(database_name)

    def drop_database(self, database_name: str) -> Result:
        return self.client._drop_db(database_name)

    def create_space(self, database_name: str, space: SpaceSchema) -> Result:
        if not self.database(database_name).exist():
            ret = self.database(database_name).create()
            if ret.code != 200:
                raise DatabaseException(code=CodeType.CREATE_DATABASE, message="create database error:" + ret.err_msg)
        url_params = {"database_name": database_name, "space_name": space.name}
        url = self.client.host + SPACE_URI % url_params
        url = self.client.host + "/dbs/%(database_name)s/spaces" % url_params
        sign = compute_sign_auth(secret=self.client.token)
        logger.debug("create space:" + url)
        logger.debug("schema:" + json.dumps(space.dict()))
        resp = requests.request(method="POST", url=url, data=json.dumps(space.dict()), auth=sign)
        logger.debug(str(resp.status_code) + resp.text)
        result = get_result(resp)
        return result

    def drop_space(self, database_name: str, space_name: str) -> Result:
        url_params = {"database_name": database_name, "space_name": space_name}
        url = self.client.host + SPACE_URI % url_params
        logger.debug("delete space url:" + url)
        sign = compute_sign_auth(secret=self.client.token)
        resp = requests.request(method="DELETE", url=url, auth=sign)
        logger.debug("delete space ret" + resp.text)
        return get_result(resp)

    def is_space_exist(self, database_name: str, space_name: str) -> [bool, SpaceSchema]:
        try:
            url_params = {"database_name": database_name, "space_name": space_name}
            url = self.client.host + SPACE_URI % url_params
            sign = compute_sign_auth(secret=self.client.token)
            resp = requests.request(method="GET", url=url, auth=sign)
            logger.debug("get space exist result:" + resp.text)
            ret = get_result(resp)
            if ret.code == 0:
                space_schema = json.dumps(ret.text)
                return True, space_schema
            else:
                return False, None
        except VearchException as e:
            if e.code == ERR_CODE_SPACE_NOT_EXIST and "notexists" in e.message:
                return False, None
            else:
                raise SpaceException(CodeType.CHECK_SPACE_EXIST, e.message)

    def create_index(self, database_name: str, space_name: str, field: str, index: Index) -> Result:
        url = self.client.host + INDEX_URI
        req_body = {"field": field, "index": index.dict(), "database": database_name, "space": space_name}
        sign = compute_sign_auth()
        req = requests.request(method="POST", url=url, data=json.dumps(req_body),
                               auth=sign)
        resp = self.client.s.send(req)
        return get_result(resp)
