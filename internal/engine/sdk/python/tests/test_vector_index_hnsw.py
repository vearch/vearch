import numpy as np
from vearch import Config, Engine, Table, FieldInfo, VectorInfo, dataType, Document, SearchRequest, VectorQuery
from vearch.schema.index import HNSWIndex, MetricType
from .utils.datasets import DatasetSift10K
from .utils.vearch_log import logger
import time

sift10k = DatasetSift10K()
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()

def search(engine, xq, k: int, batch: bool):
    results = []
    if batch:
        vector_query = VectorQuery(name="field_vector", value=xq)
        search_request = SearchRequest(vec_fields=[vector_query], limit=k)
        search_response = engine.search(search_request)
        assert search_response.code == 0
        for result in search_response.documents:
            results.append([doc.to_dict()["field_id"] for doc in result])
    else:
        for query in xq:
            vector_query = VectorQuery(name="field_vector", value=query)
            search_request = SearchRequest(vec_fields=[vector_query], limit=k)
            search_response = engine.search(search_request)
            assert search_response.code == 0
            results.append([doc.to_dict()["field_id"] for doc in search_response.documents[0]])
    return np.array(results)

def evaluate(engine, xq, gt, k, batch):
    nq = xq.shape[0]
    t0 = time.time()
    I = search(engine, xq, k, batch)
    t1 = time.time()

    recalls = {}
    i = 1
    while i <= k:
        recalls[i] = (I[:, :i] == gt[:, :1]).sum() / float(nq)
        i *= 10

    avg_query_time = (t1 - t0) * 1000.0 / nq
    return avg_query_time, recalls

def test_index_hnsw():
    config = Config(path="data", log_dir="logs")
    engine = Engine(config)
    assert engine is not None

    # Define table schema
    field_infos = [FieldInfo(name="field_id", data_type=dataType.INT, is_index=True)]
    vector_infos = [VectorInfo(name="field_vector", dimension=128)]
    table = Table(
        name="test_table",
        field_infos=field_infos,
        vector_infos=vector_infos,
        index=HNSWIndex(
            "hnsw_index",
            metric_type=MetricType.L2,
            nlinks=32,
            efConstruction=200,
            efSearch=64
        )
    )
    response = engine.create_table(table)
    assert response.code == 0

    # Insert training data with sequence numbers
    documents = []
    for i, vector in enumerate(xb):
        datas = {"field_id": i, "field_vector": vector}
        doc = Document(datas=datas)
        doc.to_fields(table)
        documents.append(doc)

    upsert_response = engine.upsert(documents)
    assert len(upsert_response.document_ids) == len(xb)

    # Evaluate recall for single query
    avg_query_time, recalls = evaluate(engine, xq, gt, k=100, batch=False)
    logger.info(f"Single query - Avg query time: {avg_query_time:.2f} ms, Recalls: {recalls}")

    # Evaluate recall for batch query
    avg_query_time, recalls = evaluate(engine, xq, gt, k=100, batch=True)
    logger.info(f"Batch query - Avg query time: {avg_query_time:.2f} ms, Recalls: {recalls}")

    engine.close()
    engine.clear()
