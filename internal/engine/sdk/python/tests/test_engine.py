import pytest
from vearch import Config, Engine, Table, FieldInfo, VectorInfo, dataType, Document, Field
from vearch.schema.index import FlatIndex, MetricType
from vearch import QueryRequest, TermFilter, RangeFilter, FilterRelationOperator, VectorQuery, SearchRequest
import numpy as np
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture(scope="class")
def setup_engine(request):
    config = Config(path="data", log_dir="logs", space_name="test_space")
    request.cls.engine = Engine(config)

@pytest.mark.usefixtures("setup_engine")
class TestEngine:
    def test_engine_initialization(self):
        assert self.engine.config.path == "data"
        assert self.engine.config.log_dir == "logs"
        assert self.engine.config.space_name == "test_space"

    def test_engine_status(self):
        status_response = self.engine.status()
        assert status_response.code == 0
        assert isinstance(status_response.to_dict()["status"], dict)

    def test_create_table(self):
        field_infos = [
            FieldInfo("field_long", dataType.LONG),
            FieldInfo("field_string", dataType.STRING, True),
            FieldInfo("field_float", dataType.FLOAT, True),
            FieldInfo("field_double", dataType.DOUBLE, True),
            FieldInfo("field_int", dataType.INT, True),
            FieldInfo("field_string_array", dataType.STRINGARRAY, True),
            FieldInfo("field_date", dataType.DATE, True)
        ]
        vector_infos = [
            VectorInfo(name="vector1", dimension=128, store_type="MemoryOnly"),
        ]
        vector_infos = [VectorInfo(name="field_vector", dimension=64)]
        index = FlatIndex("vec_index", metric_type=MetricType.L2)
        table = Table(name="test_table", field_infos=field_infos, vector_infos=vector_infos, index=index)
        response = self.engine.create_table(table)
        assert response.code == 0
        assert self.engine.table.name == "test_table"

    def test_upsert_documents(self):
        features = np.random.rand(64).astype('float32')
        datas = {
            "field_long": 1,
            "field_string": "1",
            "field_float": 1.0,
            "field_double": 1.0,
            "field_int": 1,
            "field_string_array": ["1", "1"],
            "field_date": "2020-02-02",
            "field_vector": features
        }
        doc1 = Document(datas=datas)
        doc1.to_fields(self.engine.table)

        fields = [
            Field(name="field_long", value=1, data_type=dataType.LONG),
            Field(name="field_string", value="1", data_type=dataType.STRING),
            Field(name="field_float", value=1.0, data_type=dataType.FLOAT),
            Field(name="field_double", value=1.0, data_type=dataType.DOUBLE),
            Field(name="field_int", value=1, data_type=dataType.INT),
            Field(name="field_string_array", value=["1"], data_type=dataType.STRINGARRAY),
            Field(name="field_date", value="2020-02-02", data_type=dataType.DATE),
            Field(name="field_vector", value=np.random.rand(64).astype('float32'), data_type=dataType.VECTOR)
        ]
        doc2 = Document(fields=fields)
        upsert_response = self.engine.upsert([doc1, doc2])
        assert upsert_response.code == 0
        assert len(upsert_response.document_ids) == 2

        response = self.engine.get(upsert_response.document_ids[0][0])
        assert response.code == 0
        assert response.document.to_dict()["field_string"] == "1"
        assert response.document.to_dict()["field_float"] == 1.0
        assert response.document.to_dict()["field_int"] == 1
        assert response.document.to_dict()["field_string_array"] == ["1", "1"]
        assert response.document.to_dict()["field_date"] == "2020-02-02 00:00:00"
        assert len(response.document.to_dict()["field_vector"].tolist()) == 64

        response = self.engine.get(upsert_response.document_ids[1][0])
        assert response.code == 0
        assert response.document.to_dict()["field_string"] == "1"
        assert response.document.to_dict()["field_float"] == 1.0
        assert response.document.to_dict()["field_int"] == 1
        assert response.document.to_dict()["field_string_array"] == ["1"]
        assert response.document.to_dict()["field_date"] == "2020-02-02 00:00:00"
        assert len(response.document.to_dict()["field_vector"].tolist()) == 64

    def test_query(self):
        term_filter = TermFilter(field_name="field_string", value="1", filter_operator=FilterRelationOperator.IN)
        range_filter = RangeFilter(
            field="field_float",
            lower_value=0.5,
            upper_value=1.5,
            include_lower=True,
            include_upper=False
        )
        query_request = QueryRequest(
            term_filters=[term_filter],
            range_filters=[range_filter],
            is_vector_value = True
        )
        query_response = self.engine.query(query_request)
        assert query_response.code == 0
        assert len(query_response.documents) == 2
        assert query_response.documents[0].to_dict()["field_string"] == "1"
        assert query_response.documents[0].to_dict()["field_float"] == 1.0
        assert query_response.documents[0].to_dict()["field_int"] == 1
        assert query_response.documents[0].to_dict()["field_string_array"] == ["1", "1"]
        assert query_response.documents[0].to_dict()["field_date"] == "2020-02-02 00:00:00"
        logger.info("Document: %s", query_response.documents[0].to_dict())
        logger.info("Document: %s", query_response.documents[1].to_dict())
        # assert len(query_response.documents[0].to_dict()["field_vector"].tolist()) == 64

    def test_search(self):
        vector_query = VectorQuery(name="field_vector", value=np.random.rand(1, 64).astype('float32'))
        term_filter = TermFilter(field_name="field_string", value="1", filter_operator=FilterRelationOperator.IN)
        range_filter = RangeFilter(
            field="field_float",
            lower_value=0.5,
            upper_value=1.5,
            include_lower=True,
            include_upper=False
        )
        search_request = SearchRequest(
            limit=10,
            term_filters=[term_filter],
            range_filters=[range_filter],
            vec_fields=[vector_query]
        )
        search_response = self.engine.search(search_request)
        assert search_response.code == 0
        assert len(search_response.documents) == 1
        logger.info("Search Documents: %s", [doc.to_dict() for doc in search_response.documents[0]])
        assert len(search_response.documents[0]) == 2
        assert search_response.documents[0][0].to_dict()["field_string"] == "1"
        assert search_response.documents[0][0].to_dict()["field_float"] == 1.0
        assert search_response.documents[0][0].to_dict()["field_int"] == 1
        assert search_response.documents[0][0].to_dict()["field_string_array"] == ["1", "1"] or ["1"]
        assert search_response.documents[0][0].to_dict()["field_date"] == "2020-02-02 00:00:00"
        # assert len(search_response.documents[0][0].to_dict()["field_vector"].tolist()) == 64

    def test_delete(self):
        features = np.random.rand(64).astype('float32')  # Changed to 1D array
        datas = {
            "field_long": 1,
            "field_string": "1",
            "field_float": 1.0,
            "field_double": 1.0,
            "field_int": 1,
            "field_string_array": ["1"],
            "field_date": "2020-02-02",
            "field_vector": features
        }
        doc = Document(datas=datas)
        doc.to_fields(self.engine.table)
        upsert_response = self.engine.upsert([doc])
        assert upsert_response.code == 0

        delete_response = self.engine.delete(upsert_response.document_ids[0][0])
        assert delete_response.code == 0

    def test_status(self):
        status_response = self.engine.status()
        assert status_response.code == 0

    def teardown_class(self):
        self.engine.close()
        self.engine.clear()