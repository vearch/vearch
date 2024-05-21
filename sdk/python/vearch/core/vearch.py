from vearch.config import Config
from vearch.core.db import Database
from vearch.core.space import Space
from vearch.core.client import client
from vearch.result import Result, get_result, ResultStatus, UpsertResult, SearchResult
from vearch.schema.index import Index
from vearch.schema.space import SpaceSchema
from vearch.const import SPACE_URI, INDEX_URI, UPSERT_DOC_URI, SEARCH_DOC_URI, QUERY_DOC_URI, LIST_SPACE_URI, DELETE_DOC_URI, AUTH_KEY, CODE_SPACE_NOT_EXIST, CODE_SUCCESS, MSG_NOT_EXIST
from vearch.exception import DatabaseException, VearchException, SpaceException, DocumentException
from vearch.utils import CodeType, compute_sign_auth, VectorInfo
from vearch.filter import Filter
import requests
from typing import List, Union, Optional, Dict
import pandas as pd
import json
import logging

logger = logging.getLogger("vearch")


class Vearch(object):
    def __init__(self, config: Config):
        self.client = client
        self.client.config(config)
        self._schema = None

    def create_database(self, database_name: str) -> Result:
        return self.client._create_db(database_name)

    def list_databases(self) -> List[Database]:
        result = self.client._list_db()
        l = []
        logger.debug(result.dict_str())
        if result.code == CODE_SUCCESS:
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
        if not self.is_database_exist(database_name):
            raise DatabaseException(code=CodeType.CHECK_DATABASE_EXIST, message="database not exist,can not drop it:")
        return self.client._drop_db(database_name)
 

    def create_space(self, database_name: str, space: SpaceSchema) -> Result:
        if not self.database(database_name).exist():
            ret = self.database(database_name).create()
            if ret.code != 200:
                raise DatabaseException(code=CodeType.CREATE_DATABASE, message="create database error:" + ret.err_msg)
        url_params = {"database_name": database_name, "space_name": space.name}
        url = self.client.host + LIST_SPACE_URI % url_params
        sign = compute_sign_auth(secret=self.client.token)
        logger.debug("create space:" + url)
        logger.debug("schema:" + json.dumps(space.dict()))
        resp = requests.request(method="POST", url=url, data=json.dumps(space.dict()), auth=sign)
        logger.debug("create space status_code and text:"+ str(resp.status_code) + resp.text)
        result = get_result(resp)
        return result

    def drop_space(self, database_name: str, space_name: str) -> Result:
        if not self.is_space_exist(database_name, space_name):
            raise SpaceException(CodeType.CHECK_SPACE_EXIST, message="space not exist")
        url_params = {"database_name": database_name, "space_name": space_name}
        url = self.client.host + SPACE_URI % url_params
        logger.debug("delete space url:" + url)
        sign = compute_sign_auth(secret=self.client.token)
        resp = requests.request(method="DELETE", url=url, auth=sign)
        logger.debug("delete space ret" + resp.text)
        return get_result(resp)  
           

        
    def is_space_exist(self, database_name: str, space_name: str) -> [bool, SpaceSchema]:
        try:
            if not self.is_database_exist(database_name):
                raise DatabaseException(code=CodeType.CHECK_DATABASE_EXIST, message="database not exist")
            url_params = {"database_name": database_name, "space_name": space_name}
            url = self.client.host + SPACE_URI % url_params
            sign = compute_sign_auth(secret=self.client.token)
            resp = requests.request(method="GET", url=url, auth=sign)
            logger.debug("get space exist result:" + resp.text)
            ret = get_result(resp)
            if ret.code == CODE_SUCCESS:
                space_schema = json.dumps(ret.text)
                return True, space_schema
            else:
                return False, None
        except VearchException as e:
            if e.code == CODE_SPACE_NOT_EXIST and MSG_NOT_EXIST in e.message:
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

    def list_spaces(self, database_name: str) -> List[Space]:
        result = self.client._list_space(database_name)
        l = []
        logger.debug(result.dict_str())
        if result.code == CODE_SUCCESS:
            logger.debug(result.text)
            space_names = result.text
            for space_name in space_names:
                space = Space(database_name, space_name)
                l.append(space)
            return l
        else:
            logger.error(result.dict_str())
            raise SpaceException(code=CodeType.LIST_SPACES, message="list space failed:" + result.err_msg)
            
    def delete(self, database_name: str, space_name: str,filter: Filter) -> Result:
        url = self.client.host + DELETE_DOC_URI
        req_body = {"db_name": database_name, "space_name": space_name, "filters": filter.dict()}
        logger.debug(req_body)
        sign = compute_sign_auth()
        resp = requests.request(method="POST", url=url, data=json.dumps(req_body), auth=sign)
        logger.debug(resp.__dict__)
        return get_result(resp)
    
    def upsert(self, database_name: str, space_name: str, data: Union[List, pd.DataFrame]) -> UpsertResult:
        try:
            if not self.is_space_exist(database_name, space_name):
                raise SpaceException(CodeType.CHECK_SPACE_EXIST, message="space not exist")
            space = Space(database_name, space_name)
            if not self._schema:
                has, schema = space.exist()
                if has:
                    self._schema = schema
                else:
                    raise SpaceException(CodeType.CHECK_SPACE_EXIST,
                                         "space %s not exist,please create it first" % space_name)
            url = self.client.host + UPSERT_DOC_URI
            req_body = {"db_name": database_name, "space_name": space_name}
            records = []
            if data:
                is_dataframe = isinstance(data, pd.DataFrame)
                data_fields_len = len(data.columns) if is_dataframe else len(data[0])
                if data_fields_len == len(self._schema.fields):
                    if isinstance(data, pd.DataFrame):
                        for index, row in data.iterrows():
                            record = {}

                            for i, field in enumerate(self._schema._fields):
                                record[field.name] = row[field.name]
                            records.append(record)
                    else:
                        for em in data:
                            record = {}
                            for i, field in enumerate(self._schema.fields):
                                record[field.name] = em[i]
                            records.append(record)
                    req_body.update({"documents": records})
                    logger.debug(req_body)
                    sign = compute_sign_auth(secret=self.client.token)
                    resp = requests.request(method="POST", url=url, data=json.dumps(req_body),
                                            auth=sign)
                    return UpsertResult.parse_upsert_result_from_response(resp)
                else:
                    raise DocumentException(CodeType.UPSERT_DOC, "data fields not conform space schema")
            else:
                raise DocumentException(CodeType.UPSERT_DOC, "data is empty")

        except VearchException as e:
            raise  e
            if e.code == 0:
                logger.error(e.code)
                logger.error(e.message)
                r = UpsertResult(code="200", msg=e.message)
                return r
    
    def search(self, database_name: str, space_name: str, vector_infos: Optional[List[VectorInfo]], filter: Optional[Filter] = None,
               fields: Optional[List] = None, vector: bool = False, limit: int = 50, **kwargs) -> List[Dict]:
        """
        :param vector_infos: vector infomation contains field name、feature,min score and weight.
        :param filter: through scalar fields filte result to satify the expect
        :param fields: want to return field list
        :param vector: wheather return vector or not
        :param limit:  the result size you want to return
        :param kwargs:
            "is_brute_search": 0,
            "online_log_level": "debug",
            "quick": false,
            "vector_value": false,
            "load_balance": "leader",
            "l2_sqrt": false,
            "size": 10
            retrieval_param: the retrieval parameter which control the search action,user can asign it to precisely
             control search result,different index type different parameters
             For IVFPQ:
                    "retrieval_param": {
                    "parallel_on_queries": 1,
                    "recall_num" : 100,
                    "nprobe": 80,
                    "metric_type": "L2" }
                GPU:
                    "retrieval_param": {
                    "recall_num" : 100,
                    "nprobe": 80,
                    "metric_type": "L2"}
               HNSW:
                   "retrieval_param": {
                        "efSearch": 64,
                        "metric_type": "L2"
                    }
                IVFFLAT:
                    "retrieval_param": {
                    "parallel_on_queries": 1,
                    "nprobe": 80,
                    "metric_type": "L2" }
               FLAT:
                   "retrieval_param": {
                   "metric_type": "L2"}

        :return:
        """
        if not self.is_space_exist(database_name, space_name):
            raise SpaceException(CodeType.CHECK_SPACE_EXIST, message="space not exist")
        if not vector_infos:
            raise SpaceException(CodeType.SEARCH_DOC, "vector_info can not both null")
        url = self.client.host + SEARCH_DOC_URI
        req_body = {"db_name": database_name, "space_name": space_name, "vector_value": vector, "limit": limit}
        if fields:
            req_body["fields"] = fields
        req_body["vectors"] = []
        if vector_infos:
            vector_info_dict = [vector_info.dict() for vector_info in vector_infos]
            req_body["vectors"] = vector_info_dict
        if filter:
            req_body["filters"] = filter.dict()
        if kwargs:
            req_body.update(kwargs)

        logger.debug(json.dumps(req_body))
        sign = compute_sign_auth(secret=self.client.token)
        resp = requests.request(method="POST", url=url, data=json.dumps(req_body),
                                auth=sign)
        sr = SearchResult.parse_search_result_from_response(resp)
        return sr.documents

    def query(self, database_name: str, space_name: str, document_ids: Optional[List] = [], filter: Optional[Filter] = None,
              partition_id: Optional[str] = "",
              fields: Optional[List] = [], vector: bool = False, limit: int = 50) -> List[Dict]:
        """
        you can asign  the document_ids in [xxx,xxx,xxx,xxx,xxx],or give the other filter condition.
        partition id also can be set to reduce the scope of the search space
        :param document_ids document id list
        :param expr Filter expression of filter
        :param partition_id assign which partition you want to query,default query all partitions
        :param fields the scalar fields you want to output
        :param vector return vector or not
        :param size the output result size you queried out
        :return:
        """
        if not self.is_space_exist(database_name, space_name):
            raise SpaceException(CodeType.CHECK_SPACE_EXIST, message="space not exist")
        if (not document_ids) and (not filter):
            raise SpaceException(CodeType.QUERY_DOC, "document_ids and filter can not both null")
        url = self.client.host + QUERY_DOC_URI
        req_body = {"db_name": database_name, "space_name": space_name, "vector_value": vector}
        if document_ids:
            req_body["document_ids"] = document_ids
        if partition_id:
            req_body["partition_id"] = partition_id
        if fields:
            req_body["fields"] = fields
        if filter:
            req_body["filters"] = filter.dict()
        logger.debug(url)
        logger.debug(json.dumps(req_body))
        sign = compute_sign_auth(secret=self.client.token)
        resp = requests.request(method="POST", url=url, data=json.dumps(req_body), auth=sign)
        ret = get_result(resp)
        if ret.code == CODE_SUCCESS:
            return json.dumps(ret.text)
        return []
