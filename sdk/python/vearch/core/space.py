from vearch.schema.index import Index
from vearch.core.client import client
from vearch.schema.space import SpaceSchema
from vearch.result import Result, ResultStatus, get_result
from vearch.const import SPACE_URI, INDEX_URI, UPSERT_DOC_URI, DELETE_DOC_URI, QUERY_DOC_URI
from vearch.exception import SpaceException, DocumentException
from vearch.utils import CodeType
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
        req = requests.request(method="POST", url=url, data=space.dict(), headers={"token": self.client.token})
        resp = self.client.s.send(req)
        return get_result(resp)

    def drop(self) -> Result:
        url_params = {"database_name": self.db_name, "space_name": self.name}
        url = self.client.host + SPACE_URI % url_params
        req = requests.request(method="POST", url=url, headers={"token": self.client.token})
        resp = self.client.s.send(req)
        return get_result(resp)

    def exist(self) -> [bool, SpaceSchema]:
        url_params = {"database_name": self.db_name, "space_name": self.name}
        url = self.client.host + SPACE_URI % url_params
        req = requests.request(method="GET", url=url, headers={"token": self.client.token})
        resp = self.client.s.send(req)
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
                               headers={"token": self.client.token})
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
                                   headers={"token": self.client.token})
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
        req = requests.request(method="POST", url=url, data=json.dumps(req_body), headers={"token": self.client.token})
        resp = self.client.s.send(req)
        return get_result(resp)

    def search(self):
        pass

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
        ret=get_result(resp)
        if ret.code==ResultStatus.success:
            return json.dumps(ret.content)
        return []
