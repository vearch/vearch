from vearch.schema.index import Index
from vearch.core.client import client
from vearch.schema.space import SpaceSchema
from vearch.result import Result, ResultStatus, get_result
from vearch.const import SPACE_URI, INDEX_URI, UPSERT_DOC_URI, DELETE_DOC_URI, QUERY_DOC_URI, SEARCH_DOC_URI
from vearch.exception import SpaceException, DocumentException
from vearch.utils import CodeType, VectorInfo
from vearch.filter import Filter
import requests
import json
import pandas as pd
from typing import List, Union, Optional, Dict


class Space(object):
    def __int__(self, db_name, name):
        self.db_name = db_name
        self.name = name
        self.client = client
        self._schema = None

    def create(self, space: SpaceSchema) -> Result:
        url_params = {"database_name": self.db_name, "space_name": space._name}
        url = self.client.host + SPACE_URI % url_params
        if not self._schema:
            self._schema = space
        req = requests.request(method="POST", url=url, data=space.dict(), headers={"Authorization": self.client.token})
        resp = self.client.s.send(req)
        return get_result(resp)

    def drop(self) -> Result:
        url_params = {"database_name": self.db_name, "space_name": self.name}
        url = self.client.host + SPACE_URI % url_params
        req = requests.request(method="DELETE", url=url, headers={"Authorization": self.client.token})
        resp = self.client.s.send(req)
        return get_result(resp)

    def exist(self) -> [bool, SpaceSchema]:
        url_params = {"database_name": self.db_name, "space_name": self.name}
        url = self.client.host + SPACE_URI % url_params
        resp = requests.request(method="GET", url=url, headers={"Authorization": self.client.token})
        result = get_result(resp)
        if result.code == ResultStatus.success:
            space_schema_dict = json.loads(result.content)
            space_schema = SpaceSchema.from_dict(space_schema_dict)
            return True, space_schema
        return False, None

    def create_index(self, field: str, index: Index) -> Result:
        url = self.client.host + INDEX_URI
        req_body = {"field": field, "index": index.dict(), "database": self.db_name, "space": self.name}
        req = requests.request(method="POST", url=url, data=json.dumps(req_body),
                               headers={"Authorization": self.client.token})
        resp = self.client.s.send(req)
        return get_result(resp)

    def upsert_doc(self, data: Union[List, pd.DataFrame]) -> Result:
        if not self._schema:
            has, schema = self.exist()
            if has:
                self._schema = schema
            else:
                raise SpaceException(CodeType.CHECK_SPACE_EXIST,
                                     "space %s not exist,please create it first" % self.name)
        url = self.client.host + UPSERT_DOC_URI
        req_body = {"database": self.db_name, "space": self.name}
        records = []
        if self._check_data_conforms_schema(data):
            if isinstance(data, pd.DataFrame):
                for index, row in data.iterrows():
                    record = {}
                    for i, field in enumerate(self._schema._fileds):
                        record[field.name] = row[field.name]
                    records.append(record)
            else:
                for em in data:
                    record = {}
                    for i, field in enumerate(self._schema._fileds):
                        record[field.name] = em[i]
                    records.append(record)
            req_body.update({"data": records})
            req = requests.request(method="POST", url=url, data=json.dumps(req_body),
                                   headers={"Authorization": self.client.token})
            resp = self.client.s.send(req)
            return get_result(resp)
        else:
            raise DocumentException(CodeType.UPSERT_DOC, "data fields not conform space schema")

    def _check_data_conforms_schema(self, data: Union[List, pd.DataFrame]) -> bool:
        if data:
            is_dataframe = isinstance(data, pd.DataFrame)
            data_fields_len = len(data.columns) if is_dataframe else len(data[0])
            return data_fields_len == len(self._schema._fileds)
        else:
            raise DocumentException(CodeType.UPSERT_DOC, "data is empty")
        return True

    def delete_doc(self, filter: Filter) -> Result:
        url = self.client.host + DELETE_DOC_URI
        req_body = {"database": self.db_name, "space": self.name, "filter": filter.dict()}
        req = requests.request(method="POST", url=url, data=json.dumps(req_body), headers={"Authorization": self.client.token})
        resp = self.client.s.send(req)
        return get_result(resp)

    def search(self, document_ids: Optional[List], vector_infos: Optional[List[VectorInfo]], filter: Optional[Filter],
               fields: Optional[List] = [], vector: bool = False, size: int = 50, **kwargs) -> List[Dict]:
        """

        :param document_ids: document_ids asigned will first to query its feature in the database,then ann search
        :param vector_infos: vector infomation contains field name、feature,min score and weight.
        :param filter: through scalar fields filte result to satify the expect
        :param fields: want to return field list
        :param vector: wheather return vector or not
        :param size:  the result size you want to return
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
        if (not document_ids) and (not vector_infos):
            raise SpaceException(CodeType.SEARCH_DOC, "document_ids and vector_info can not both null")
        url = self.client.host + SEARCH_DOC_URI
        req_body = {"database": self.db_name, "space": self.name, "vector_value": vector, "size": size}
        if fields:
            req_body["fields"] = fields
        query = {"query": {}}
        if document_ids:
            query["query"]["document_ids"] = document_ids
        if vector_infos:
            vector_info_dict = [vector_info.dict() for vector_info in vector_infos]
            query["query"]["vector"] = vector_info_dict
        if filter:
            query["query"]["filter"] = filter.dict()
        req_body.update(query)
        if kwargs:
            req_body.update(kwargs)
        req = requests.request(method="POST", url=url, data=json.dumps(req_body), headers={"Authorization": self.client.token})
        resp = self.client.s.send(req)
        ret = get_result(resp)
        if ret.code != ResultStatus.success:
            raise SpaceException(CodeType.SEARCH_DOC, ret.err_msg)
        search_ret = json.dumps(ret.code)
        return search_ret

    def query(self, document_ids: Optional[List], filter: Optional[Filter], partition_id: Optional[str] = "",
              fields: Optional[List] = [], vector: bool = False, size: int = 50) -> List[Dict]:
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
        if (not document_ids) and (not filter):
            raise SpaceException(CodeType.QUERY_DOC, "document_ids and filter can not both null")
        url = self.client.host + QUERY_DOC_URI
        req_body = {"database": self.db_name, "space": self.name, "vector_value": vector, "size": size}
        query = {"query": {}}
        if document_ids:
            query["query"]["document_ids"] = document_ids
        if partition_id:
            query["query"]["partition_id"] = partition_id
        if fields:
            req_body["fields"] = fields
        if filter:
            query["query"]["filter"] = filter.dict()
        req_body.update(query)
        req = requests.request(method="POST", url=url, data=json.dumps(req_body), headers={"token": self.client.token})
        resp = self.client.s.send(req)
        ret = get_result(resp)
        if ret.code == ResultStatus.success:
            return json.dumps(ret.content)
        return []
