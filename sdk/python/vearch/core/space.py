from vearch.schema.index import Index
from vearch.core.client import client
from vearch.schema.space import SpaceSchema
from vearch.result import Result, ResultStatus, get_result
from vearch.const import SPACE_URI, INDEX_URI, UPSERT_DOC_URI
from vearch.exception import SpaceException, DocumentException
from vearch.utils import CodeType
import requests
import json
import pandas as pd
from typing import List, Union, Dict


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

    def upsert_doc(self, data: Union[List, pd.DataFrame, Dict]) -> Result:
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

    def delete_doc(self):
        pass

    def search(self):
        pass

    def query(self):
        pass
