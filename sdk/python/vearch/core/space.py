import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

from vearch.const import (
    CODE_SPACE_NOT_EXIST,
    MSG_NOT_EXIST,
)
from vearch.core.client import RestClient
from vearch.exception import SpaceException, VearchException
from vearch.filter import Filter
from vearch.result import (
    DeleteResult,
    Result,
    SearchResult,
    UpsertResult,
)
from vearch.schema.index import Index
from vearch.schema.space import SpaceSchema
from vearch.utils import (
    CodeType,
    UpsertDataType,
    VectorInfo,
)

logger = logging.getLogger("vearch")


class Space(object):
    def __init__(self, db_name: str, space_name: str, client: RestClient):
        self.database_name = db_name
        self.name = space_name
        self.client = client
        self._schema = None

    def create(self, space: SpaceSchema) -> Result:
        self._schema = space
        return self.client._create_space(self.database_name, space)

    def drop(self) -> Result:
        return self.client._drop_space(self.database_name, self.name)

    def exist(self) -> Tuple[bool, SpaceSchema]:
        try:
            result = self.client._get_space_detail(self.database_name, self.name)
            if result.is_success():
                space_schema = SpaceSchema.from_dict(result.data)
                self._schema = space_schema
                return True, space_schema
            else:
                return False, None
        except VearchException as e:
            if e.code == CODE_SPACE_NOT_EXIST and MSG_NOT_EXIST in e.message:
                return False, None
            else:
                raise SpaceException(CodeType.CHECK_SPACE_EXIST, e.message)

    def create_index(self, field: str, index: Index) -> Result:
        return self.client._create_index(self.database_name, self.name, field, index)

    def upsert(self, data: Union[List, pd.DataFrame]) -> UpsertResult:
        if not self._schema:
            has, schema = self.exist()
            if has:
                self._schema = schema
            else:
                return UpsertResult(
                    CodeType.CHECK_SPACE_EXIST,
                    "space %s not exist, please create it first" % self.name,
                )

        data_type, err_msg = self._check_data_type(data)

        if data_type == UpsertDataType.DATA_FRAME:
            documents = []
            for row in data.itertuples(index=False):
                record = {field.name: row[field.name] for field in self._schema._fields}
                documents.append(record)

        elif data_type == UpsertDataType.LIST_MAP:
            documents = data

        elif data_type == UpsertDataType.LIST:
            documents = []
            for item in data:
                record = {
                    field.name: item[i] for i, field in enumerate(self._schema.fields)
                }
                documents.append(record)
        else:
            return UpsertResult(CodeType.UPSERT_DOC, "data type has error: " + err_msg)

        return self.client._upsert(self.database_name, self.name, documents)

    def _check_data_type(
        self, data: Union[List, pd.DataFrame]
    ) -> Tuple[UpsertDataType, str]:
        if data is None or len(data) == 0:
            return UpsertDataType.ERROR, "data is null"

        item_num = len(data)
        item_dict_num = 0
        item_list_num = 0
        if isinstance(data, pd.DataFrame):
            if len(data.columns) == len(self._schema.fields):
                return UpsertDataType.DATA_FRAME, ""
            else:
                return (
                    UpsertDataType.ERROR,
                    "pandas.DataFrame column num should equal to space schema fields",
                )
        elif isinstance(data, List):
            for item in data:
                if isinstance(item, dict):
                    item_dict_num = item_dict_num + 1
                if len(item) == len(self._schema.fields):
                    item_list_num = item_list_num + 1
            if item_num == item_dict_num:
                return UpsertDataType.LIST_MAP, ""
            elif item_num == item_list_num:
                return UpsertDataType.LIST, ""
            else:
                return (
                    UpsertDataType.ERROR,
                    "data item length should equal to space schema fields",
                )
        else:
            return UpsertDataType.ERROR, "data type should be list or pandas.DataFrame"

    def delete(
        self,
        document_ids: Optional[List[str]] = None,
        filter: Optional[Filter] = None,
        limit: int = 50,
    ) -> DeleteResult:
        return self.client._delete_documents(
            self.database_name, self.name, document_ids, filter, limit
        )

    def search(
        self,
        vector_infos: Optional[List[VectorInfo]],
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

        return self.client._search_documents(
            self.database_name,
            self.name,
            vector_infos,
            filter,
            fields,
            vector,
            limit,
            **kwargs,
        )

    def query(
        self,
        document_ids: Optional[List] = None,
        filter: Optional[Filter] = None,
        partition_id: Optional[int] = None,
        fields: Optional[List] = [],
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

        return self.client._query_documents(
            self.database_name,
            self.name,
            document_ids,
            filter,
            partition_id,
            fields,
            vector,
            limit,
        )

