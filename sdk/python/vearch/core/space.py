from vearch.schema.index import Index
from vearch.core.client import client
from vearch.schema.space import SpaceSchema
from vearch.result import (
    Result,
    ResultStatus,
    get_result,
    UpsertResult,
    SearchResult,
    DeleteResult,
)
from vearch.const import (
    LIST_SPACE_URI,
    SPACE_URI,
    INDEX_URI,
    UPSERT_DOC_URI,
    DELETE_DOC_URI,
    QUERY_DOC_URI,
    SEARCH_DOC_URI,
    CODE_SPACE_NOT_EXIST,
    AUTH_KEY,
    CODE_SUCCESS,
    MSG_NOT_EXIST,
)
from vearch.exception import SpaceException, DocumentException, VearchException
from vearch.utils import (
    CodeType,
    VectorInfo,
    compute_sign_auth,
    DataType,
    UpsertDataType,
)
from vearch.filter import Filter
import requests
import json
import pandas as pd
from typing import List, Union, Optional, Dict
import logging

logger = logging.getLogger("vearch")


class Space(object):
    def __init__(self, db_name, name):
        self.db_name = db_name
        self.name = name
        self.client = client
        self._schema = None

    def create(self, space: SpaceSchema) -> Result:
        url_params = {"database_name": self.db_name, "space_name": space.name}
        url = self.client.host + LIST_SPACE_URI % url_params
        if not self._schema:
            self._schema = space
        sign = compute_sign_auth(secret=self.client.token)
        resp = requests.request(
            method="POST", url=url, data=json.dumps(space.dict()), auth=sign
        )
        return get_result(resp)

    def drop(self) -> Result:
        url_params = {"database_name": self.db_name, "space_name": self.name}
        url = self.client.host + SPACE_URI % url_params
        sign = compute_sign_auth(secret=self.client.token)
        resp = requests.request(method="DELETE", url=url, auth=sign)
        return get_result(resp)

    def exist(self) -> [bool, SpaceSchema]:
        try:
            url_params = {"database_name": self.db_name, "space_name": self.name}
            uri = SPACE_URI % url_params
            url = self.client.host + str(uri)
            sign = compute_sign_auth(secret=self.client.token)
            resp = requests.request(method="GET", url=url, auth=sign)
            result = get_result(resp)
            if result.code == CODE_SUCCESS:
                space_schema_dict = result.data
                space_schema = SpaceSchema.from_dict(space_schema_dict)
                return True, space_schema
            else:
                return False, None
        except VearchException as e:
            if e.code == CODE_SPACE_NOT_EXIST and MSG_NOT_EXIST in e.message:
                return False, None
            else:
                raise SpaceException(CodeType.CHECK_SPACE_EXIST, e.message)

    def create_index(self, field: str, index: Index) -> Result:
        url = self.client.host + INDEX_URI
        req_body = {
            "field": field,
            "index": index.dict(),
            "database": self.db_name,
            "space": self.name,
        }
        sign = compute_sign_auth(secret=self.client.token)
        resp = requests.request(
            method="POST", url=url, data=json.dumps(req_body), auth=sign
        )
        return get_result(resp)

    def upsert(self, data: Union[List, pd.DataFrame]) -> UpsertResult:
        try:
            if not self._schema:
                has, schema = self.exist()
                if has:
                    self._schema = schema
                else:
                    return UpsertResult(
                        CodeType.CHECK_SPACE_EXIST,
                        "space %s not exist, please create it first" % self.name,
                    )
            url = self.client.host + UPSERT_DOC_URI
            req_body = {"db_name": self.db_name, "space_name": self.name}
            records = []
            data_type, err_msg = self._check_data_type(data)

            if data_type != UpsertDataType.ERROR:
                if isinstance(data, pd.DataFrame):
                    for index, row in data.iterrows():
                        record = {}
                        for i, field in enumerate(self._schema._fields):
                            record[field.name] = row[field.name]
                        records.append(record)
                else:
                    if data_type == UpsertDataType.LIST_MAP:
                        req_body.update({"documents": data})

                    if data_type == UpsertDataType.LIST:
                        for em in data:
                            record = {}
                            for i, field in enumerate(self._schema.fields):
                                record[field.name] = em[i]
                            records.append(record)
                        req_body.update({"documents": records})

                sign = compute_sign_auth(secret=self.client.token)
                resp = requests.request(
                    method="POST", url=url, data=json.dumps(req_body), auth=sign
                )
                return UpsertResult.parse_upsert_result_from_response(resp)
            else:
                return UpsertResult(
                    CodeType.UPSERT_DOC, "data type has error: " + err_msg
                )
        except VearchException as e:
            raise e

    def _check_data_type(self, data: Union[List, pd.DataFrame]) -> List[str]:
        if data == None or len(data) == 0:
            return UpsertDataType.ERROR, "data is null"

        is_dataframe = isinstance(data, pd.DataFrame)

        data_fields_len = len(data.columns) if is_dataframe else len(data[0])
        item_num = len(data)
        item_dict_num = 0
        item_list_num = 0
        if is_dataframe:
            if len(data.columns) == len(self._schema.fields):
                return UpsertDataType.DATA_FRAME, ""
            else:
                return (
                    UpsertDataType.ERROR,
                    "pandas.DataFrame column num should equal to space schema fields",
                )

        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    item_dict_num = item_dict_num + 1
                if len(item) == len(self._schema.fields):
                    item_list_num = item_list_num + 1
            if item_num == item_dict_num:
                return UpsertDataType.LIST_MAP, ""
            else:
                if item_num == item_list_num:
                    return UpsertDataType.LIST, ""
                else:
                    return (
                        UpsertDataType.ERROR,
                        "data item length should equal to space schema fields",
                    )
        else:
            return UpsertDataType.ERROR, "data type should be list or pandas.DataFrame"

    def _check_data_conforms_schema(self, data: Union[List, pd.DataFrame]) -> bool:
        if data:
            is_dataframe = isinstance(data, pd.DataFrame)
            data_fields_len = len(data.columns) if is_dataframe else len(data[0])
            return data_fields_len == len(self._schema.fields)
        else:
            return False
        return True

    def delete(
        self,
        document_ids: Optional[List] = [],
        filter: Optional[Filter] = None,
        limit: int = 50,
    ) -> Result:
        url = self.client.host + DELETE_DOC_URI
        req_body = {"db_name": self.db_name, "space_name": self.name, "limit": limit}
        if document_ids:
            req_body["document_ids"] = document_ids
        if filter:
            req_body["filters"] = filter.dict()

        sign = compute_sign_auth()
        resp = requests.request(
            method="POST", url=url, data=json.dumps(req_body), auth=sign
        )
        return DeleteResult.parse_delete_result_from_response(resp)

    def search(
        self,
        vector_infos: Optional[List[VectorInfo]],
        filter: Optional[Filter] = None,
        fields: Optional[List] = None,
        vector: bool = False,
        limit: int = 50,
        **kwargs
    ) -> List[Dict]:
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
        if not vector_infos:
            return SearchResult(CodeType.SEARCH_DOC, "vector_info can not both null")
        url = self.client.host + SEARCH_DOC_URI
        req_body = {
            "db_name": self.db_name,
            "space_name": self.name,
            "vector_value": vector,
            "limit": limit,
        }
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

        sign = compute_sign_auth(secret=self.client.token)
        resp = requests.request(
            method="POST", url=url, data=json.dumps(req_body), auth=sign
        )
        return SearchResult.parse_search_result_from_response(resp)

    def query(
        self,
        document_ids: Optional[List] = [],
        filter: Optional[Filter] = None,
        partition_id: Optional[int] = None,
        fields: Optional[List] = [],
        vector: bool = False,
        limit: int = 50,
    ) -> List[Dict]:
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
        url = self.client.host + QUERY_DOC_URI
        req_body = {
            "db_name": self.db_name,
            "space_name": self.name,
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
        sign = compute_sign_auth(secret=self.client.token)
        resp = requests.request(
            method="POST", url=url, data=json.dumps(req_body), auth=sign
        )
        return SearchResult.parse_search_result_from_response(resp)
