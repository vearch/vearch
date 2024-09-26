from __future__ import annotations

import logging
from typing import List, Optional

import requests
from requests.adapters import HTTPAdapter

from vearch.config import (
    DEFAULT_MAX_CONNECTIONS,
    DEFAULT_RETRIES,
    DEFAULT_TIMEOUT,
    DEFAULT_TOKEN,
    Config,
)
from vearch.const import (
    DATABASE_URI,
    DELETE_DOC_URI,
    INDEX_URI,
    LIST_DATABASE_URI,
    LIST_SPACE_URI,
    QUERY_DOC_URI,
    SEARCH_DOC_URI,
    SPACE_URI,
    UPSERT_DOC_URI,
)
from vearch.filter import Filter
from vearch.result import DeleteResult, Result, SearchResult, UpsertResult, get_result
from vearch.schema.index import Index
from vearch.schema.space import SpaceSchema
from vearch.utils import CodeType, VectorInfo, compute_sign_auth

logger = logging.getLogger("vearch")


class RestClient(object):
    @classmethod
    def from_config(cls, config: Config) -> RestClient:
        return cls(
            host=config.host,
            max_connections=config.max_connections,
            max_retries=config.max_retries,
            token=config.token,
            timeout=config.timeout,
        )

    def __init__(
        self,
        host: str,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
        max_retries: int = DEFAULT_RETRIES,
        token: str = DEFAULT_TOKEN,
        timeout: int = DEFAULT_TIMEOUT,
    ):
        httpAdapter = HTTPAdapter(
            pool_maxsize=max_connections,
            max_retries=max_retries,
        )
        s = requests.Session()
        s.mount("http://", adapter=httpAdapter)
        self.s = s
        self.host = host
        self.token = token
        self.timeout = timeout

    def config(self, config: Config):
        httpAdapter = HTTPAdapter(
            pool_maxsize=config.max_connections, max_retries=config.max_retries
        )
        s = requests.Session()
        s.mount("http://", adapter=httpAdapter)
        self.s = s
        self.host = config.host
        self.token = config.token
        self.timeout = config.timeout

    def _create_db(self, database_name: str) -> Result:
        url_params = {"database_name": database_name}
        url = self.host + DATABASE_URI % url_params
        sign = compute_sign_auth(secret=self.token)
        resp = self.s.request(method="POST", url=url, auth=sign)
        return get_result(resp)

    def _drop_db(self, database_name: str) -> Result:
        url_params = {"database_name": database_name}
        url = self.host + (DATABASE_URI % url_params)
        sign = compute_sign_auth(secret=self.token)
        resp = self.s.request(method="DELETE", url=url, auth=sign)
        return get_result(resp)

    def _list_db(self) -> Result:
        url = self.host + LIST_DATABASE_URI
        sign = compute_sign_auth(secret=self.token)
        resp = self.s.request(method="GET", url=url, auth=sign)
        return get_result(resp)

    def _get_db_detail(self, database_name: str) -> Result:
        url_params = {"database_name": database_name}
        url = self.host + (DATABASE_URI % url_params)
        sign = compute_sign_auth(secret=self.token)
        resp = self.s.request(method="GET", url=url, auth=sign)
        return get_result(resp)

    def _list_space(self, database_name: str) -> Result:
        url_params = {"database_name": database_name}
        url = self.host + (LIST_SPACE_URI % url_params)
        sign = compute_sign_auth(secret=self.token)
        resp = self.s.request(method="GET", url=url, auth=sign)
        return get_result(resp)

    def _create_space(self, database_name: str, space_schema: SpaceSchema) -> Result:
        url_params = {"database_name": database_name}
        url = self.host + LIST_SPACE_URI % url_params
        sign = compute_sign_auth(secret=self.token)
        resp = self.s.request(
            method="POST", url=url, json=space_schema.dict(), auth=sign
        )
        return get_result(resp)

    def _drop_space(self, database_name: str, space_name: str) -> Result:
        url_params = {"database_name": database_name, "space_name": space_name}
        url = self.host + (SPACE_URI % url_params)
        sign = compute_sign_auth(secret=self.token)
        resp = self.s.request(method="DELETE", url=url, auth=sign)
        return get_result(resp)

    def _get_space_detail(self, database_name: str, space_name: str) -> Result:
        url_params = {"database_name": database_name, "space_name": space_name}
        url = self.host + (SPACE_URI % url_params)
        sign = compute_sign_auth(secret=self.token)
        resp = self.s.request(method="GET", url=url, auth=sign)
        return get_result(resp)

    def _create_index(
        self, database_name: str, space_name: str, field: str, index: Index
    ) -> Result:
        url = self.host + INDEX_URI
        req_body = {
            "field": field,
            "index": index.dict(),
            "database": database_name,
            "space": space_name,
        }
        sign = compute_sign_auth(secret=self.token)
        resp = self.s.request(method="POST", url=url, json=req_body, auth=sign)
        return get_result(resp)

    def _upsert(
        self, database_name: str, space_name: str, documents: List
    ) -> UpsertResult:
        url = self.host + UPSERT_DOC_URI
        req_body = {
            "db_name": database_name,
            "space_name": space_name,
            "documents": documents,
        }

        sign = compute_sign_auth(secret=self.token)
        resp = self.s.request(method="POST", url=url, json=req_body, auth=sign)
        return UpsertResult.parse_upsert_result_from_response(resp)

    def _delete_documents(
        self,
        database_name: str,
        space_name: str,
        document_ids: Optional[List[str]] = None,
        filter: Optional[Filter] = None,
        limit: int = 50,
    ) -> DeleteResult:
        url = self.host + DELETE_DOC_URI
        req_body = {
            "db_name": database_name,
            "space_name": space_name,
            "limit": limit,
        }
        if document_ids:
            req_body["document_ids"] = document_ids
        if filter:
            req_body["filters"] = filter.dict()

        sign = compute_sign_auth()
        resp = self.s.request(method="POST", url=url, json=req_body, auth=sign)
        return DeleteResult.parse_delete_result_from_response(resp)

    def _query_documents(
        self,
        database_name: str,
        space_name: str,
        document_ids: Optional[List] = None,
        filter: Optional[Filter] = None,
        partition_id: Optional[int] = None,
        fields: Optional[List] = None,
        vector: bool = False,
        limit: int = 50,
    ) -> SearchResult:
        """
        you can asign  the document_ids in [xxx,xxx,xxx,xxx,xxx],or give the other filter condition.
        partition id also can be set to reduce the scope of the search space
        :param document_ids document id list
        :param expr Filter expression of filter
        :param partition_id assign which partition you want to query,default query all partitions
        :param fields the scalar fields you want to output
        :param vector return vector or not
        :param limit the output result size you queried out
        :return:
        """
        if (not document_ids) and (not filter):
            return SearchResult(
                CodeType.QUERY_DOC, "document_ids and filter can not both null"
            )
        url = self.host + QUERY_DOC_URI
        req_body = {
            "db_name": database_name,
            "space_name": space_name,
            "vector_value": vector,
            "limit": limit,
        }
        if document_ids:
            req_body["document_ids"] = document_ids
        if partition_id:
            req_body["partition_id"] = partition_id
        if fields:
            req_body["fields"] = fields
        if filter:
            req_body["filters"] = filter.dict()
        sign = compute_sign_auth(secret=self.token)
        resp = self.s.request(method="POST", url=url, json=req_body, auth=sign)
        return SearchResult.parse_search_result_from_response(resp)

    def _search_documents(
        self,
        database_name: str,
        space_name: str,
        vector_infos: List[VectorInfo],
        filter: Optional[Filter] = None,
        fields: Optional[List[str]] = None,
        vector: bool = False,
        limit: int = 50,
        **kwargs,
    ) -> SearchResult:
        """
        :param vector_infos: vector infomation contains field name、feature,min score and weight.
        :param filter: through scalar fields filte result to satify the expect
        :param fields: want to return field list
        :param vector: wheather return vector or not
        :param limit:  the result size you want to return
        :param kwargs:
            "is_brute_search": 0,
            "vector_value": false,
            "load_balance": "leader",
            "l2_sqrt": false,
            "limit": 10

            retrieval_param: the retrieval parameter which control the search action,user can asign it to precisely
             control search result,different index type different parameters
             For IVFPQ:
                "index_params": {
                "parallel_on_queries": 1,
                "recall_num" : 100,
                "nprobe": 80,
                "metric_type": "L2" }
                GPU:
                "index_params": {
                "recall_num" : 100,
                "nprobe": 80,
                "metric_type": "L2"}
               HNSW:
               "index_params": {
                    "efSearch": 64,
                    "metric_type": "L2"
                }
                IVFFLAT:
                "index_params": {
                "parallel_on_queries": 1,
                "nprobe": 80,
                "metric_type": "L2" }
               FLAT:
               "index_params": {
               "metric_type": "L2"}

        :return:
        """
        if len(vector_infos) == 0:
            return SearchResult(CodeType.SEARCH_DOC, "vector_info can not null")

        url = self.host + SEARCH_DOC_URI
        req_body = {
            "db_name": database_name,
            "space_name": space_name,
            "limit": limit,
            "vectors": [vector_info.dict() for vector_info in vector_infos],
            "vector_value": vector,
            **kwargs,
        }
        if fields:
            req_body["fields"] = fields
        if filter:
            req_body["filters"] = filter.dict()

        sign = compute_sign_auth(secret=self.token)
        resp = self.s.request(method="POST", url=url, json=req_body, auth=sign)
        return SearchResult.parse_search_result_from_response(resp)
