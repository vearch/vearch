import logging
from typing import List
import json
import pytest
import random
import time
from vearch.config import Config
from vearch.core.vearch import Vearch
from vearch.schema.field import Field
from vearch.schema.space import SpaceSchema
from vearch.utils import DataType, MetricType, VectorInfo, CodeType
from vearch.schema.index import (
    IvfPQIndex,
    Index,
    ScalarIndex,
    HNSWIndex,
    IvfFlatIndex,
    BinaryIvfIndex,
    FlatIndex,
    GPUIvfPQIndex,
)
from vearch.filter import Filter, Condition, FieldValue, Conditions
from vearch.exception import (
    DatabaseException,
    VearchException,
    SpaceException,
    DocumentException,
)
from vearch.core.client import RestClient
from config import test_host_url

logger = logging.getLogger("vearch_test")

database_name = "database_test_v"
database_name1 = "database_test_not_exist"
space_name = "book_info"
space_name1 = "book_infonot_exist"


vc = Vearch(Config(host=test_host_url))

vi = VectorInfo("book_character", [random.uniform(0, 1) for _ in range(512)])
conditons_search = [
    Condition(operator=">", fv=FieldValue(field="book_num", value=0)),
]
search_filters = Filter(operator="AND", conditions=conditons_search)


class TestVearchBadcase(object):
    @pytest.mark.parametrize(
        "database_name",
        [
            ("database_test_v"),
        ],
    )
    def test_is_database_exist_init(self, database_name):
        ret = vc.is_database_exist(database_name)
        assert ret == False

    @pytest.mark.parametrize(
        "database_name",
        [
            ("database_test_v"),
        ],
    )
    def test_create_database(self, database_name):
        logger.debug(vc.client.host)
        ret = vc.create_database(database_name)
        logger.debug(ret.dict_str())
        assert ret.__dict__["code"] in [0, 1]

    @pytest.mark.parametrize(
        "database_name",
        [
            ("database_test_v"),
        ],
    )
    def test_is_database_exist_again(self, database_name):
        ret = vc.is_database_exist(database_name)
        assert ret == True

    @pytest.mark.parametrize(
        "database_name",
        [
            ("database_test_v"),
        ],
    )
    def test_create_database_repeated(self, database_name):
        try:
            logger.debug(vc.client.host)
            ret = vc.create_database(database_name)
        except VearchException as e:
            assert e.code in [0, 1]

    def test_list_databases(self):
        logger.debug(vc.client.host)
        ret = vc.list_databases()
        logger.debug(ret)
        assert len(ret) >= 0

    book_name = Field(
        "book_name",
        DataType.STRING,
        desc="the name of book",
        index=ScalarIndex("book_name_idx"),
    )
    book_num = Field(
        "book_num",
        DataType.INTEGER,
        desc="the num of book",
        index=ScalarIndex("book_num_idx"),
    )

    @pytest.mark.parametrize(
        "database_name, space_schema",
        [
            (
                "database_test_v",
                SpaceSchema(
                    "book_info_ivfpq",
                    [
                        book_name,
                        book_num,
                        Field(
                            "book_character",
                            DataType.VECTOR,
                            IvfPQIndex(
                                "book_vec_idx", MetricType.Inner_product, 2048, 8, 10000
                            ),
                            dimension=512,
                        ),
                    ],
                ),
            ),
            (
                "database_test_v",
                SpaceSchema(
                    "book_info_ivfflat",
                    [
                        book_name,
                        book_num,
                        Field(
                            "book_character",
                            DataType.VECTOR,
                            IvfFlatIndex(
                                "book_vec_idx", MetricType.Inner_product, 2048
                            ),
                            dimension=512,
                        ),
                    ],
                ),
            ),
            # ("database_test_v",SpaceSchema("book_info_biivf", [book_name, book_num, Field("book_character", DataType.VECTOR, BinaryIvfIndex("book_vec_idx", 2048), dimension=512)])),
            (
                "database_test_v",
                SpaceSchema(
                    "book_info_flat",
                    [
                        book_name,
                        book_num,
                        Field(
                            "book_character",
                            DataType.VECTOR,
                            FlatIndex("book_vec_idx", MetricType.Inner_product),
                            dimension=512,
                        ),
                    ],
                ),
            ),
            (
                "database_test_v",
                SpaceSchema(
                    "book_info_hnsw",
                    [
                        book_name,
                        book_num,
                        Field(
                            "book_character",
                            DataType.VECTOR,
                            HNSWIndex("book_vec_idx", MetricType.Inner_product, 32, 40),
                            dimension=512,
                        ),
                    ],
                ),
            ),
        ],
    )
    def test_create_space(self, database_name, space_schema):
        ret = vc.create_space(database_name, space_schema)
        assert ret.code in [220, 0]

    # @pytest.mark.parametrize('database_name, space_schema',
    #                          [
    # ("database_new",SpaceSchema("book_info_ivfpq", [book_name, book_num, Field("book_character", DataType.VECTOR, IvfPQIndex("book_vec_idx", MetricType.Inner_product, 2048, 8), dimension=512)])),
    #                          ])
    # def test_create_space_badcase(self,database_name,space_schema):
    #     ret = vc.create_space(database_name, space_schema)
    #     assert ret.code in [CodeType.CREATE_DATABASE]

    @pytest.mark.parametrize(
        "database_name, space_name",
        [
            ("database_test_v", "book_info_ivfpq"),
            ("database_test_v", "book_info_biiv"),
            ("db_test", "book_info_ivfpq"),
        ],
    )
    def test_is_space_exist(self, database_name, space_name):
        ret = vc.is_space_exist(database_name, space_name)
        logger.debug(ret)
        assert ret[0] in [True, False]

    @pytest.mark.parametrize(
        "database_name",
        [
            ("database_test_v"),
            (""),
        ],
    )
    def test_list_spaces(self, database_name):
        logger.debug(vc.client.host)
        ret = vc.list_spaces(database_name)
        logger.debug(ret)
        assert len(ret) >= 0

    book_name_template = [
        "qdbwjfwv",
        "acwlvvq",
        "cwvwvqqc",
        "cwvwvveq",
        "cevqbgnd",
        "vrhuofnm",
        "wkvwveve",
        "cwbtjimu",
    ]
    num = [12, 34, 56, 74, 53, 11, 14, 9]
    data_complete = []
    data_miss = []
    for i in range(8):
        book_item = [
            book_name_template[i],
            num[i],
            [random.uniform(0, 1) for _ in range(512)],
        ]
        book_item_m = [
            book_name_template[i],
            [random.uniform(0, 1) for _ in range(512)],
        ]
        data_complete.append(book_item)
        data_miss.append(book_item_m)

    @pytest.mark.parametrize(
        "database_name, space_name, data",
        [
            ("database_test_v", "book_info_ivfpq", data_complete),
        ],
    )
    def test_upsert_doc(self, database_name, space_name, data) -> List:
        ret = vc.upsert(database_name, space_name, data)
        logger.debug(f"upsert doc:{ret.document_ids}")
        assert ret.code == 0

    @pytest.mark.parametrize(
        "database_name, space_name, data",
        [
            ("database_test_v", "book_info_ivfpq", data_miss),
            ("database_test_v", "book_info_ivfpq", []),
            ("database_test_v", "book_info_ivpq", data_complete),
            ("db_test", "book_info_ivfpq", data_complete),
        ],
    )
    def test_upsert_doc_badcase(self, database_name, space_name, data) -> List:
        ret = vc.upsert(database_name, space_name, data)
        assert ret.code in [CodeType.UPSERT_DOC, CodeType.CHECK_SPACE_EXIST]

    conditons = [
        Condition(operator="<", fv=FieldValue(field="book_num", value=25)),
        Condition(operator=">", fv=FieldValue(field="book_num", value=12)),
    ]
    delete_filters = Filter(operator="AND", conditions=conditons)

    @pytest.mark.parametrize(
        "database_name, space_name, ids,filters",
        [
            ("database_test_v", "book_info_ivfpq", None, delete_filters),
            ("database_test_v", "book_info_ivfpq", ["chjebvbevbhejbve"], None),
        ],
    )
    def test_delete_doc(self, database_name, space_name, ids, filters):
        time.sleep(1)
        ret = vc.delete(database_name, space_name, ids, filter=filters)
        logger.debug(f"delete doc:{ret.document_ids}")
        assert ret.code == 0

    conditons_query = [
        Condition(operator=">", fv=FieldValue(field="book_num", value=0)),
        Condition(
            operator="IN",
            fv=FieldValue(
                field="book_name", value=["bpww57nu", "sykboivx", "edjn9542"]
            ),
        ),
    ]
    query_filters = Filter(operator="AND", conditions=conditons_query)

    @pytest.mark.parametrize(
        "database_name, space_name, ids, filters",
        [
            ("database_test_v", "book_info_ivfpq", [], query_filters),
            ("database_test_v", "book_info_ivfpq", ["chjebvbevbhejbve"], None),
        ],
    )
    def test_query(self, database_name, space_name, ids, filters):

        ret = vc.query(database_name, space_name, ids, filters)
        logger.debug(f"query doc:{ret.documents}")
        assert ret.code == 0

    @pytest.mark.parametrize(
        "database_name, space_name, ids, filters",
        [
            ("db_test", "book_info_ivfpq", ["chjebvbevbhejbve"], None),
            ("database_test_v", "book_info_ivpq", ["chjebvbevbhejbve"], None),
            ("database_test_v", "book_info_ivfpq", None, None),
        ],
    )
    def test_query_badcase(self, database_name, space_name, ids, filters):

        ret = vc.query(database_name, space_name, ids, filters)
        assert ret.code in [6, 0, CodeType.QUERY_DOC]

    @pytest.mark.parametrize(
        "database_name, space_name, vec_info,filters,limit",
        [
            (
                "database_test_v",
                "book_info_ivfpq",
                [
                    vi,
                ],
                search_filters,
                3,
            ),
        ],
    )
    def test_search(self, database_name, space_name, vec_info, filters, limit):
        ret = vc.search(
            database_name,
            space_name,
            vector_infos=[
                vi,
            ],
            filter=filters,
            limit=limit,
        )
        logger.debug(f"search doc:{ret.documents}")
        assert ret.code == 0

    @pytest.mark.parametrize(
        "database_name, space_name, vec_info,filters,limit",
        [
            (
                "db_test",
                "book_info_ivfpq",
                [
                    vi,
                ],
                search_filters,
                3,
            ),
            (
                "db_test",
                "book_info_ivfpq",
                [
                    vi,
                ],
                None,
                None,
            ),
            (
                "db_test",
                "book_info_ivpq",
                [
                    vi,
                ],
                None,
                None,
            ),
        ],
    )
    def test_search_badcase(self, database_name, space_name, vec_info, filters, limit):
        ret = vc.search(
            database_name,
            space_name,
            vector_infos=[
                vi,
            ],
            filter=filters,
            limit=limit,
        )
        assert ret.code == 6

    @pytest.mark.parametrize(
        "database_name, space_name",
        [
            ("database_test_v", "book_info_ivpq"),
            ("db_test", "book_info_ivfflat"),
        ],
    )
    def test_drop_space_badcase(self, database_name, space_name):
        ret = vc.drop_space(database_name, space_name)
        assert ret.__dict__["code"] in [221, 220, 200]

    @pytest.mark.parametrize(
        "database_name, space_name",
        [
            ("database_test_v", "book_info_ivfpq"),
            ("database_test_v", "book_info_ivfflat"),
            ("database_test_v", "book_info_flat"),
            ("database_test_v", "book_info_hnsw"),
        ],
    )
    def test_drop_space(self, database_name, space_name):
        ret = vc.drop_space(database_name, space_name)
        assert ret.code == 0

    @pytest.mark.parametrize(
        "database_name",
        [
            ("database_test_v"),
        ],
    )
    def test_drop_database(self, database_name):
        ret = vc.drop_database(database_name)
        assert ret.code == 0
