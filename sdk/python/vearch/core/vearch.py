import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

from vearch.config import Config
from vearch.const import (
    CODE_SPACE_NOT_EXIST,
    MSG_NOT_EXIST,
)
from vearch.core.client import RestClient
from vearch.core.db import Database
from vearch.core.space import Space
from vearch.exception import (
    DatabaseException,
    SpaceException,
    VearchException,
)
from vearch.filter import Filter
from vearch.result import (
    DeleteResult,
    Result,
    SearchResult,
    UpsertResult,
)
from vearch.schema.index import Index
from vearch.schema.space import SpaceSchema
from vearch.utils import CodeType, VectorInfo

logger = logging.getLogger("vearch")


class Vearch(object):
    def __init__(self, config: Config):
        self.client = RestClient.from_config(config)

    def database(self, database_name: str) -> Database:
        return Database(database_name, self.client)

    def list_databases(self) -> List[Database]:
        result = self.client._list_db()
        databases = []
        if result.is_success():
            database_datas = result.data
            for database_data in database_datas:
                databases.append(self.database(database_data["name"]))
            return databases
        else:
            raise DatabaseException(
                code=CodeType.LIST_DATABASES,
                message="list database failed:" + result.msg,
            )

    def create_database(self, database_name: str) -> Result:
        return self.database(database_name).create()

    def is_database_exist(self, database_name: str) -> bool:
        return self.database(database_name).exist()

    def drop_database(self, database_name: str) -> Result:
        return self.database(database_name).drop()

    def space(self, database_name: str, space_name: str) -> Space:
        return Space(database_name, space_name, self.client)

    def list_spaces(self, database_name: str) -> List[Space]:
        return self.database(database_name).list_spaces()

    def create_space(self, database_name: str, space: SpaceSchema) -> Result:
        return self.space(database_name, space.name).create(space)

    def drop_space(self, database_name: str, space_name: str) -> Result:
        return self.space(database_name, space_name).drop()

    def is_space_exist(
        self, database_name: str, space_name: str
    ) -> Tuple[bool, Result, SpaceSchema]:
        try:
            if not self.is_database_exist(database_name):
                return (
                    False,
                    Result(
                        code=CodeType.CHECK_DATABASE_EXIST,
                        msg="database %s not exist" % (database_name),
                    ),
                    None,
                )
            result = self.client._get_space_detail(database_name, space_name)
            if result.is_success():
                space_schema = SpaceSchema.from_dict(result.data)
                return True, result, space_schema
            else:
                return False, result, None
        except VearchException as e:
            if e.code == CODE_SPACE_NOT_EXIST and MSG_NOT_EXIST in e.message:
                return False, Result(code=e.code, msg=e.message), None
            else:
                raise SpaceException(CodeType.CHECK_SPACE_EXIST, e.message)

    def create_index(
        self, database_name: str, space_name: str, field: str, index: Index
    ) -> Result:
        return self.space(database_name, space_name).create_index(field, index)

    def upsert(
        self,
        database_name: str,
        space_name: str,
        data: Union[List[Union[Dict, Any]], pd.DataFrame],
    ) -> UpsertResult:
        space = self.space(database_name, space_name)
        return space.upsert(data)

    def search(
        self,
        database_name: str,
        space_name: str,
        vector_infos: List[VectorInfo],
        filter: Optional[Filter] = None,
        fields: Optional[List] = None,
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
            index_params: the retrieval parameter which control the search action,user can asign it to precisely
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
        space = self.space(database_name, space_name)
        return space.search(vector_infos, filter, fields, vector, limit, **kwargs)

    def query(
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
        space = self.space(database_name, space_name)
        return space.query(document_ids, filter, partition_id, fields, vector, limit)

    def delete(
        self,
        database_name: str,
        space_name: str,
        document_ids: Optional[List] = [],
        filter: Optional[Filter] = None,
        limit: int = 50,
    ) -> DeleteResult:
        space = self.space(database_name, space_name)
        return space.delete(document_ids, filter, limit)
