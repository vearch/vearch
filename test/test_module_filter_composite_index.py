#
# Copyright 2019 The Vearch Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

# -*- coding: UTF-8 -*-

"""
test case for composite scalar index filter

Covers all combinations of filter modes across composite index fields:
  - Eq (single-value equality)
  - IN  (multi-value list)
  - Range (inclusive bounds)
  - Combinations of Eq + Range + IN across prefix / suffix positions
  - Edge cases: empty suffix, empty prefix, single-field composite

Composite index: [field_A (integer), field_B (integer), field_C (string)]
Data layout:
  - field_A: sequential 0..(total_docs-1)
  - field_B: sequential 0..(total_docs-1)  (same as field_A, for easy expected-value calc)
  - field_C: generated from field_B modulo num_c_values
  - field_vector: random 128-dim float32 vector

Key scenarios:
  1. Eq on prefix only       → Equal("A=v")
  2. Eq on all fields        → Equal("A=v1|B=v2|C=v3")
  3. Range on prefix field   → Range("[A>=lo,A<=hi]")
  4. Range on last field     → Range with suffix min/max
  5. IN on prefix            → Cartesian In
  6. IN on suffix            → Cartesian In with Range driver or pure Cartesian
  7. Range + Eq in suffix    → Range driver branch
  8. Range + IN in suffix    → Range driver + in-memory filter
  9. All three: Eq + Range + IN
 10. Empty result conditions
"""

import json
import numpy as np
import pytest
import requests

from utils.vearch_utils import (
    create_db,
    create_space,
    destroy,
    get_space_num,
    logger,
    router_url,
    username,
    password,
    db_name,
)

__description__ = "test composite scalar index filter"


# ---------------------------------------------------------------------------
# Data generation (module-scoped cache)
# ---------------------------------------------------------------------------

TOTAL_DOCS = 1000
EMBEDDING_DIM = 128
NUM_C_VALUES = 20        # cardinality of field_C
FIELD_B_SCALE = 10        # field_B = field_A * FIELD_B_SCALE

_cached_vectors = None
_cached_field_a = None
_cached_field_b = None
_cached_field_c = None


def _generate_data():
    global _cached_vectors, _cached_field_a, _cached_field_b, _cached_field_c
    if _cached_vectors is not None:
        return _cached_vectors, _cached_field_a, _cached_field_b, _cached_field_c

    rng = np.random.default_rng(seed=42)
    _cached_vectors = rng.random((TOTAL_DOCS, EMBEDDING_DIM), dtype=np.float32)
    _cached_field_a = np.arange(TOTAL_DOCS, dtype=np.int32)
    _cached_field_b = (_cached_field_a * FIELD_B_SCALE).astype(np.int32)
    _cached_field_c = np.array(
        [str(i % NUM_C_VALUES) for i in _cached_field_b], dtype=object
    )
    return _cached_vectors, _cached_field_a, _cached_field_b, _cached_field_c


# ---------------------------------------------------------------------------
# Space helpers
# ---------------------------------------------------------------------------

_COMPOSITE_INDEX_SPACE_NAME = "composite_0_space"
_vector_dim = EMBEDDING_DIM


def _build_composite_space_config(scalar_index_type: str = "INVERTED"):
    """Build space with composite index [field_A, field_B, field_C].

    Consecutive COMPOSITE fields form one composite index.
    field_C is string type so it covers both numeric and string range.
    """
    fields = [
        {
            "name": "field_A",
            "type": "integer",
        },
        {
            "name": "field_B",
            "type": "integer",
        },
        {
            "name": "field_C",
            "type": "string",
        },
        {
            "name": "field_vector",
            "type": "vector",
            "dimension": _vector_dim,
            "store_type": "MemoryOnly",
            "index": {
                "name": "gamma",
                "type": "FLAT",
                "params": {"metric_type": "L2"},
            },
        },
    ]
    indexes = [
        {"name": "idx_composite_s0",  "type": "COMPOSITE", "field_names": ["field_A", "field_B", "field_C"]}
    ]
    space_config = {
        "name": _COMPOSITE_INDEX_SPACE_NAME,
        "partition_num": 1,
        "replica_num": 1,
        "fields": fields,
        "indexes": indexes,
    }
    return space_config


def _do_build_index(scalar_index_type: str = "INVERTED"):
    """Create space and build composite index synchronously."""
    _do_destroy_index()
    response = create_db(router_url, db_name)
    assert response.json()["code"] == 0, f"create_db failed: {response.json()}"
    space_config = _build_composite_space_config(scalar_index_type)
    response = create_space(router_url, db_name, space_config)
    assert response.json()["code"] == 0, f"create_space failed: {response.json()}"


def _do_destroy_index():
    response = destroy(router_url, db_name, _COMPOSITE_INDEX_SPACE_NAME)
    if response is not None and response.status_code == 200:
        logger.info(f"destroyed space: {_COMPOSITE_INDEX_SPACE_NAME}")


def _upsert_documents(docs):
    """Upsert documents via HTTP API."""
    url = router_url + "/document/upsert?timeout=120000"
    payload = {
        "db_name": db_name,
        "space_name": _COMPOSITE_INDEX_SPACE_NAME,
        "documents": docs,
    }
    rs = requests.post(url, auth=(username, password), json=payload)
    assert rs.status_code == 200, f"upsert failed: {rs.text}"
    assert rs.json()["code"] == 0, f"upsert code != 0: {rs.json()}"


def _batch_upsert(batch_size: int = 100):
    """Upsert all TOTAL_DOCS documents in batches."""
    vectors, field_a, field_b, field_c = _generate_data()
    docs = []
    for i in range(TOTAL_DOCS):
        docs.append({
            "_id": str(i),
            "field_A": int(field_a[i]),
            "field_B": int(field_b[i]),
            "field_C": str(field_c[i]),
            "field_vector": vectors[i].tolist(),
        })
        if len(docs) >= batch_size or i == TOTAL_DOCS - 1:
            _upsert_documents(docs)
            docs = []


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def _query(filters: dict, limit: int = 10, fields: list = None):
    """Execute document query and return documents list."""
    payload = {
        "db_name": db_name,
        "space_name": _COMPOSITE_INDEX_SPACE_NAME,
        "filters": filters,
        "limit": limit,
    }
    if fields:
        payload["fields"] = fields
    url = router_url + "/document/query"
    rs = requests.post(url, auth=(username, password), json=payload)
    assert rs.status_code == 200, f"query failed: {rs.text}"
    assert rs.json()["code"] == 0, f"query code != 0: {rs.json()}"
    return rs.json()["data"]["documents"]


# ---------------------------------------------------------------------------
# Expected value computation
# ---------------------------------------------------------------------------

def _expected_docs(a_lo=None, a_hi=None, a_eq=None,
                    b_lo=None, b_hi=None, b_eq=None,
                    c_eq=None, c_in=None):
    """Compute expected document IDs matching the given constraints.

    All parameters are inclusive bounds.
    Returns sorted list of doc IDs.
    """
    vectors, field_a, field_b, field_c = _generate_data()
    result = []
    for i in range(TOTAL_DOCS):
        a, b, c = field_a[i], field_b[i], str(field_c[i])
        if a_lo is not None and a < a_lo:
            continue
        if a_hi is not None and a > a_hi:
            continue
        if a_eq is not None and a != a_eq:
            continue
        if b_lo is not None and b < b_lo:
            continue
        if b_hi is not None and b > b_hi:
            continue
        if b_eq is not None and b != b_eq:
            continue
        if c_eq is not None and c != c_eq:
            continue
        if c_in is not None and c not in c_in:
            continue
        result.append(i)
    return result


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def composite_space():
    """Setup: create space, build composite index, upsert data."""
    logger.info("Setting up schema-0 (int+int+string) test space...")
    _do_build_index()
    _batch_upsert(batch_size=100)
    count = get_space_num(space_name=_COMPOSITE_INDEX_SPACE_NAME)
    logger.info(f"Space document count after upsert: {count}")
    assert count == TOTAL_DOCS, f"Expected {TOTAL_DOCS}, got {count}"
    yield
    logger.info("Tearing down composite index test space...")
    _do_destroy_index()


# Ensure the module-scoped fixture runs even without per-method requests.
@pytest.fixture(scope="module", autouse=True)
def _request_composite_space(composite_space):
    pass


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

# ----- Eq on prefix only ---------------------------------------------------

class TestIntIntString:
    """field_A = value  (single prefix field matched)"""

    def test_eq_prefix_0(self):
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "=", "value": 0}
            ]},
            limit=10
        )
        assert len(docs) == 1
        assert int(docs[0]["field_A"]) == 0

    def test_eq_prefix_middle(self):
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "=", "value": 500}
            ]},
            limit=10
        )
        assert len(docs) == 1
        assert int(docs[0]["field_A"]) == 500

    def test_eq_prefix_no_match(self):
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "=", "value": TOTAL_DOCS + 100}
            ]},
            limit=10
        )
        assert len(docs) == 0

    def test_not_eq_prefix_limit_10(self):
        limit = 10
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "!=", "value": 0}
            ]},
            limit=limit
        )
        assert len(docs) == limit

    def test_not_eq_prefix_limit_total_docs(self):
        limit = TOTAL_DOCS
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "!=", "value": 0}
            ]},
            limit=limit
        )
        assert len(docs) == TOTAL_DOCS - 1

    def test_not_eq_prefix_middle_limit_10(self):
        limit = 10
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "!=", "value": 500}
            ]},
            limit=limit
        )
        assert len(docs) == limit

    def test_not_eq_prefix_middle_limit_total_docs(self):
        limit = TOTAL_DOCS
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "!=", "value": 500}
            ]},
            limit=limit
        )
        assert len(docs) == TOTAL_DOCS - 1

    def test_not_eq_prefix_no_match_limit_10(self):
        limit = 10
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "!=", "value": TOTAL_DOCS + 100}
            ]},
            limit=limit
        )
        assert len(docs) == limit

    def test_not_eq_prefix_no_match_limit_total_docs(self):
        limit = TOTAL_DOCS
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "!=", "value": TOTAL_DOCS + 100}
            ]},
            limit=limit
        )
        assert len(docs) == limit

# ----- Eq on all fields -----------------------------------------------------
    """field_A = v1 AND field_B = v2 AND field_C = v3"""

    def test_eq_all_match(self):
        a_val, b_val = 42, 42 * FIELD_B_SCALE
        c_val = str(b_val % NUM_C_VALUES)
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "=", "value": a_val},
                {"field": "field_B", "operator": "=", "value": b_val},
                {"field": "field_C", "operator": "IN", "value": [c_val]},
            ]},
            limit=10
        )
        assert len(docs) == 1
        assert int(docs[0]["field_A"]) == a_val
        assert int(docs[0]["field_B"]) == b_val
        assert docs[0]["field_C"] == c_val

    def test_eq_all_match_b_first(self):
        a_val, b_val = 42, 42 * FIELD_B_SCALE
        c_val = str(b_val % NUM_C_VALUES)
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_B", "operator": "=", "value": b_val},
                {"field": "field_A", "operator": "=", "value": a_val},
                {"field": "field_C", "operator": "IN", "value": [c_val]},
            ]},
            limit=10
        )
        assert len(docs) == 1
        assert int(docs[0]["field_A"]) == a_val
        assert int(docs[0]["field_B"]) == b_val
        assert docs[0]["field_C"] == c_val

    def test_eq_all_match_c_first(self):
        a_val, b_val = 42, 42 * FIELD_B_SCALE
        c_val = str(b_val % NUM_C_VALUES)
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_C", "operator": "IN", "value": [c_val]},
                {"field": "field_A", "operator": "=", "value": a_val},
                {"field": "field_B", "operator": "=", "value": b_val},
            ]},
            limit=10
        )
        assert len(docs) == 1
        assert int(docs[0]["field_A"]) == a_val
        assert int(docs[0]["field_B"]) == b_val
        assert docs[0]["field_C"] == c_val

    def test_eq_all_no_match(self):
        # field_B does not match field_A * FIELD_B_SCALE
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "=", "value": 10},
                {"field": "field_B", "operator": "=", "value": 99},   # mismatch
                {"field": "field_C", "operator": "IN", "value": ["5"]},
            ]},
            limit=10
        )
        assert len(docs) == 0


# ----- Range on prefix field -----------------------------------------------
    """field_A >= lo AND field_A <= hi"""

    def test_range_prefix_single_value(self):
        lo, hi = 100, 100
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": ">=", "value": lo},
                {"field": "field_A", "operator": "<=", "value": hi},
            ]},
            limit=TOTAL_DOCS
        )
        expected = _expected_docs(a_lo=lo, a_hi=hi)
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_range_prefix_10_values(self):
        lo, hi = 200, 209
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": ">=", "value": lo},
                {"field": "field_A", "operator": "<=", "value": hi},
            ]},
            limit=TOTAL_DOCS
        )
        expected = _expected_docs(a_lo=lo, a_hi=hi)
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        logger.info(f"doc_ids: {doc_ids}")
        assert doc_ids == expected

    def test_range_prefix_half_dataset(self):
        lo, hi = 0, TOTAL_DOCS // 2 - 1
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": ">=", "value": lo},
                {"field": "field_A", "operator": "<=", "value": hi},
            ]},
            limit=TOTAL_DOCS
        )
        expected = _expected_docs(a_lo=lo, a_hi=hi)
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        logger.info(f"doc_ids: {doc_ids}")
        assert doc_ids == expected

    def test_range_prefix_both_open(self):
        # field_A > 100 AND field_A < 200  =>  101..199
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": ">", "value": 100},
                {"field": "field_A", "operator": "<", "value": 200},
            ]},
            limit=TOTAL_DOCS
        )
        expected = _expected_docs(a_lo=101, a_hi=199)
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        logger.info(f"doc_ids: {doc_ids}")
        assert doc_ids == expected

    def test_range_prefix_open_lower(self):
        # field_A > 50  =>  51..end
        lo, hi = 51, TOTAL_DOCS - 1
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": ">", "value": 50},
            ]},
            limit=TOTAL_DOCS
        )
        expected = _expected_docs(a_lo=lo)
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        logger.info(f"doc_ids: {doc_ids}")
        assert doc_ids == expected

    def test_range_prefix_open_upper(self):
        # field_A < 50  =>  0..49
        hi = 49
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "<", "value": 50},
            ]},
            limit=TOTAL_DOCS
        )
        expected = _expected_docs(a_hi=hi)
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        logger.info(f"doc_ids: {doc_ids}")
        assert doc_ids == expected

    def test_range_prefix_no_match(self):
        lo = TOTAL_DOCS
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": ">=", "value": lo},
            ]},
            limit=10
        )
        assert len(docs) == 0

# ----- Range on prefix field + Eq on suffix field -----------------------------------------------
    """field_A >= lo AND field_A <= hi AND (field_B = v1 OR field_C in [v2])"""

    def test_range_prefix_eq_suffix(self):
        lo, hi = 10, 20
        b_val = lo * FIELD_B_SCALE
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": ">=", "value": lo},
                {"field": "field_A", "operator": "<=", "value": hi},
                {"field": "field_B", "operator": "=", "value": b_val},
            ]},
            limit=TOTAL_DOCS
        )
        assert len(docs) == 0

    def test_range_prefix_eq_all_suffix(self):
        lo, hi = 30, 40
        b_val = lo * FIELD_B_SCALE
        c_vals = [str(lo % NUM_C_VALUES)]
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": ">=", "value": lo},
                {"field": "field_A", "operator": "<=", "value": hi},
                {"field": "field_B", "operator": "=", "value": b_val},
                {"field": "field_C", "operator": "IN", "value": c_vals},
            ]},
            limit=TOTAL_DOCS
        )
        assert len(docs) == 0

# ----- Range on prefix field + Range on suffix field -----------------------------------------------
    """field_A >= lo AND field_A <= hi AND ((field_B >= v1 and field_B <= v1) OR field_C in [v2])"""

    def test_range_prefix_range_suffix(self):
        lo, hi = 50, 60
        b_lo, b_hi = lo * FIELD_B_SCALE, hi * FIELD_B_SCALE
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": ">=", "value": lo},
                {"field": "field_A", "operator": "<=", "value": hi},
                {"field": "field_B", "operator": ">=", "value": b_lo},
                {"field": "field_B", "operator": "<=", "value": b_hi},
            ]},
            limit=TOTAL_DOCS
        )
        assert len(docs) == 0

    def test_range_prefix_range_suffix_no_match(self):
        lo, hi = 70, 80
        b_hi = lo * FIELD_B_SCALE
        b_lo = b_hi + 1
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": ">=", "value": lo},
                {"field": "field_A", "operator": "<=", "value": hi},
                {"field": "field_B", "operator": ">=", "value": b_lo},
                {"field": "field_B", "operator": "<=", "value": b_hi},
            ]},
            limit=TOTAL_DOCS
        )
        assert len(docs) == 0

    def test_range_prefix_range_suffix_with_in_suffix(self):
        lo, hi = 210, 250
        b_lo = lo * FIELD_B_SCALE
        b_hi = hi * FIELD_B_SCALE
        c_vals = [str(lo % NUM_C_VALUES)]
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": ">=", "value": lo},
                {"field": "field_A", "operator": "<=", "value": hi},
                {"field": "field_B", "operator": ">=", "value": b_lo},
                {"field": "field_B", "operator": "<=", "value": b_hi},
                {"field": "field_C", "operator": "IN", "value": c_vals},
            ]},
            limit=TOTAL_DOCS
        )
        assert len(docs) == 0

    def test_range_prefix_range_all_suffix(self):
        lo, hi = 310, 350
        b_lo, b_hi = lo * FIELD_B_SCALE, hi * FIELD_B_SCALE
        c_vals = [str(lo % NUM_C_VALUES), str((hi) % NUM_C_VALUES)]
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": ">=", "value": lo},
                {"field": "field_A", "operator": "<=", "value": hi},
                {"field": "field_B", "operator": ">=", "value": b_lo},
                {"field": "field_B", "operator": "<=", "value": b_hi},
                {"field": "field_C", "operator": "IN", "value": c_vals},
            ]},
            limit=TOTAL_DOCS
        )
        assert len(docs) == 0

# ----- Eq prefix + Range suffix (no suffix filter) --------------------------
    """field_A = v AND field_B IN [b_lo..b_hi]  (suffix empty after B, Range on B)"""

    def test_eq_a_range_b_suffix_empty(self):
        a_val = 50
        b_lo = 200
        b_hi = 250
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "=", "value": a_val},
                {"field": "field_B", "operator": ">=", "value": b_lo},
                {"field": "field_B", "operator": "<=", "value": b_hi},
            ]},
            limit=TOTAL_DOCS
        )
        # field_B = field_A * FIELD_B_SCALE, so only a_val=50 => b=500
        # 200 <= 500 <= 250 is FALSE, so result should be empty
        expected = _expected_docs(a_eq=a_val, b_lo=b_lo, b_hi=b_hi)
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_a_range_b_suffix_empty_b_first(self):
        a_val = 50
        b_lo = 200
        b_hi = 250
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_B", "operator": ">=", "value": b_lo},
                {"field": "field_B", "operator": "<=", "value": b_hi},
                {"field": "field_A", "operator": "=", "value": a_val},
            ]},
            limit=TOTAL_DOCS
        )
        # field_B = field_A * FIELD_B_SCALE, so only a_val=50 => b=500
        # 200 <= 500 <= 250 is FALSE, so result should be empty
        expected = _expected_docs(a_eq=a_val, b_lo=b_lo, b_hi=b_hi)
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_a_range_b_suffix_empty_a_middle(self):
        a_val = 50
        b_lo = 200
        b_hi = 250
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_B", "operator": ">=", "value": b_lo},
                {"field": "field_A", "operator": "=", "value": a_val},
                {"field": "field_B", "operator": "<=", "value": b_hi},
            ]},
            limit=TOTAL_DOCS
        )
        # field_B = field_A * FIELD_B_SCALE, so only a_val=50 => b=500
        # 200 <= 500 <= 250 is FALSE, so result should be empty
        expected = _expected_docs(a_eq=a_val, b_lo=b_lo, b_hi=b_hi)
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_a_range_b_overlap(self):
        # Pick a range that overlaps with the scaled value
        a_val = 50          # => field_B = 500
        b_lo = 400
        b_hi = 600          # 500 is inside
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "=", "value": a_val},
                {"field": "field_B", "operator": ">=", "value": b_lo},
                {"field": "field_B", "operator": "<=", "value": b_hi},
            ]},
            limit=TOTAL_DOCS
        )
        expected = _expected_docs(a_eq=a_val, b_lo=b_lo, b_hi=b_hi)
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_a_range_b_overlap_b_first(self):
        # Pick a range that overlaps with the scaled value
        a_val = 50          # => field_B = 500
        b_lo = 400
        b_hi = 600          # 500 is inside
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_B", "operator": ">=", "value": b_lo},
                {"field": "field_B", "operator": "<=", "value": b_hi},
                {"field": "field_A", "operator": "=", "value": a_val},
            ]},
            limit=TOTAL_DOCS
        )
        expected = _expected_docs(a_eq=a_val, b_lo=b_lo, b_hi=b_hi)
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_a_range_b_overlap_b_middle(self):
        # Pick a range that overlaps with the scaled value
        a_val = 50          # => field_B = 500
        b_lo = 400
        b_hi = 600          # 500 is inside
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_B", "operator": ">=", "value": b_lo},
                {"field": "field_A", "operator": "=", "value": a_val},
                {"field": "field_B", "operator": "<=", "value": b_hi},
            ]},
            limit=TOTAL_DOCS
        )
        expected = _expected_docs(a_eq=a_val, b_lo=b_lo, b_hi=b_hi)
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_a_range_b_half_open_lower(self):
        """field_A = v AND field_B >= lo (half-open lower)"""
        a_val = 50          # => field_B = 500
        b_lo = 500
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "=", "value": a_val},
                {"field": "field_B", "operator": ">=", "value": b_lo},
            ]},
            limit=TOTAL_DOCS
        )
        expected = _expected_docs(a_eq=a_val, b_lo=b_lo)
        assert len(docs) == len(expected)
        assert len(docs) == 1

    def test_eq_a_range_b_half_open_lower_b_first(self):
        """field_A = v AND field_B >= lo (half-open lower)"""
        a_val = 50          # => field_B = 500
        b_lo = 500
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_B", "operator": ">=", "value": b_lo},
                {"field": "field_A", "operator": "=", "value": a_val},
            ]},
            limit=TOTAL_DOCS
        )
        expected = _expected_docs(a_eq=a_val, b_lo=b_lo)
        assert len(docs) == len(expected)
        assert len(docs) == 1

    def test_eq_a_range_b_half_open_upper(self):
        """field_A = v AND field_B < hi (half-open upper)"""
        a_val = 50          # => field_B = 500
        b_hi = 500
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "=", "value": a_val},
                {"field": "field_B", "operator": "<", "value": b_hi},
            ]},
            limit=TOTAL_DOCS
        )
        expected = _expected_docs(a_eq=a_val, b_hi=b_hi - 1)
        assert len(docs) == len(expected)
        assert len(docs) == 0

    def test_eq_a_range_b_open_lower(self):
        """field_A = v AND field_B > lo (open lower)"""
        a_val = 50          # => field_B = 500
        b_lo = 500
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "=", "value": a_val},
                {"field": "field_B", "operator": ">", "value": b_lo},
            ]},
            limit=TOTAL_DOCS
        )
        expected = _expected_docs(a_eq=a_val, b_lo=b_lo + 1)
        assert len(docs) == len(expected)
        assert len(docs) == 0

    def test_eq_a_range_b_open_upper(self):
        """field_A = v AND field_B < hi (open upper)"""
        a_val = 50          # => field_B = 500
        b_hi = 500
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "=", "value": a_val},
                {"field": "field_B", "operator": "<", "value": b_hi},
            ]},
            limit=TOTAL_DOCS
        )
        expected = _expected_docs(a_eq=a_val, b_hi=b_hi - 1)
        assert len(docs) == len(expected)
        assert len(docs) == 0


# ----- IN on suffix ---------------------------------------------------------
    """field_C IN {v1, v2}  (suffix IN, prefix empty)"""

    def test_in_suffix(self):
        """field_C IN with valid values"""
        c_vals = ["0", "10"]   # only values field_C can have
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_C", "operator": "IN", "value": c_vals}
            ]},
            limit=TOTAL_DOCS
        )
        assert len(docs) == 0

    def test_in_suffix(self):
        """field_C IN with valid values"""
        c_vals = ["3", "5"]   # field_C do not hava
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_C", "operator": "IN", "value": c_vals}
            ]},
            limit=TOTAL_DOCS
        )
        assert len(docs) == 0

#  ----- Eq prefix + IN suffix ------------------------------------------------
    """field_A = v AND field_C IN {c1, c2}"""

    def test_eq_a_in_c(self):
        a_val = 100
        c_vals = ["5", "15"]
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "=", "value": a_val},
                {"field": "field_C", "operator": "IN", "value": c_vals},
            ]},
            limit=TOTAL_DOCS
        )
        expected = _expected_docs(a_eq=a_val, c_in=c_vals)
        assert len(docs) == 0

    def test_eq_a_in_c_no_match(self):
        a_val = 100
        c_vals = ["999"]     # impossible value
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "=", "value": a_val},
                {"field": "field_C", "operator": "IN", "value": c_vals},
            ]},
            limit=TOTAL_DOCS
        )
        assert len(docs) == 0


# ----- All three: Eq + Range + IN -------------------------------------------
    """field_A = v AND field_B IN [b_lo..b_hi] AND field_C IN {c1, c2}"""

    def test_eq_range_in_all(self):
        a_val = 100
        b_lo, b_hi = 400, 600
        c_vals = ["5", "15"]
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "=", "value": a_val},
                {"field": "field_B", "operator": ">=", "value": b_lo},
                {"field": "field_B", "operator": "<=", "value": b_hi},
                {"field": "field_C", "operator": "IN", "value": c_vals},
            ]},
            limit=TOTAL_DOCS
        )
        # a_val=100 => b=1000, not in [400,600], so expected is empty
        expected = _expected_docs(a_eq=a_val, b_lo=b_lo, b_hi=b_hi, c_in=c_vals)
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_range_in_all_b_first(self):
        a_val = 100
        b_lo, b_hi = 400, 600
        c_vals = ["5", "15"]
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_B", "operator": ">=", "value": b_lo},
                {"field": "field_A", "operator": "=", "value": a_val},
                {"field": "field_B", "operator": "<=", "value": b_hi},
                {"field": "field_C", "operator": "IN", "value": c_vals},
            ]},
            limit=TOTAL_DOCS
        )
        # a_val=100 => b=1000, not in [400,600], so expected is empty
        expected = _expected_docs(a_eq=a_val, b_lo=b_lo, b_hi=b_hi, c_in=c_vals)
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_range_in_all_c_first(self):
        a_val = 100
        b_lo, b_hi = 400, 600
        c_vals = ["5", "15"]
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_C", "operator": "IN", "value": c_vals},
                {"field": "field_B", "operator": ">=", "value": b_lo},
                {"field": "field_A", "operator": "=", "value": a_val},
                {"field": "field_B", "operator": "<=", "value": b_hi},
            ]},
            limit=TOTAL_DOCS
        )
        # a_val=100 => b=1000, not in [400,600], so expected is empty
        expected = _expected_docs(a_eq=a_val, b_lo=b_lo, b_hi=b_hi, c_in=c_vals)
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_range_in_all_overlap(self):
        a_val = 50           # => b=500
        b_lo, b_hi = 400, 600   # 500 in range
        c_val = str(500 % NUM_C_VALUES)   # field_C for b=500
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": "=", "value": a_val},
                {"field": "field_B", "operator": ">=", "value": b_lo},
                {"field": "field_B", "operator": "<=", "value": b_hi},
                {"field": "field_C", "operator": "IN", "value": [c_val]},
            ]},
            limit=TOTAL_DOCS
        )
        expected = _expected_docs(a_eq=a_val, b_lo=b_lo, b_hi=b_hi, c_in=[c_val])
        assert len(docs) == 0

    def test_eq_range_in_all_overlap_b_first(self):
        a_val = 50           # => b=500
        b_lo, b_hi = 400, 600   # 500 in range
        c_val = str(500 % NUM_C_VALUES)   # field_C for b=500
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_B", "operator": ">=", "value": b_lo},
                {"field": "field_A", "operator": "=", "value": a_val},
                {"field": "field_B", "operator": "<=", "value": b_hi},
                {"field": "field_C", "operator": "IN", "value": [c_val]},
            ]},
            limit=TOTAL_DOCS
        )
        expected = _expected_docs(a_eq=a_val, b_lo=b_lo, b_hi=b_hi, c_in=[c_val])
        assert len(docs) == 0

    def test_eq_range_in_all_overlap_b_first(self):
        a_val = 50           # => b=500
        b_lo, b_hi = 400, 600   # 500 in range
        c_val = str(500 % NUM_C_VALUES)   # field_C for b=500
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_C", "operator": "IN", "value": [c_val]},
                {"field": "field_B", "operator": ">=", "value": b_lo},
                {"field": "field_A", "operator": "=", "value": a_val},
                {"field": "field_B", "operator": "<=", "value": b_hi},
            ]},
            limit=TOTAL_DOCS
        )
        expected = _expected_docs(a_eq=a_val, b_lo=b_lo, b_hi=b_hi, c_in=[c_val])
        assert len(docs) == 0


# ----- Empty result edge cases ---------------------------------------------
    """Conditions that should produce zero results."""

    def test_range_no_overlap(self):
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": ">=", "value": TOTAL_DOCS + 100},
                {"field": "field_A", "operator": "<=", "value": TOTAL_DOCS + 200},
            ]},
            limit=10
        )
        assert len(docs) == 0

    def test_conflicting_range(self):
        # lo > hi: impossible range
        docs = _query(
            {"operator": "AND", "conditions": [
                {"field": "field_A", "operator": ">=", "value": 500},
                {"field": "field_A", "operator": "<=", "value": 100},
            ]},
            limit=10
        )
        assert len(docs) == 0


# Schema 1: string + int + float  (float as suffix)
_STR_INT_FLOAT_NAME = "composite_s1_space"

_cached_s1 = None


def _generate_s1_data():
    global _cached_s1
    if _cached_s1 is not None:
        return _cached_s1
    rng = np.random.default_rng(seed=99)
    vectors = rng.random((TOTAL_DOCS, EMBEDDING_DIM), dtype=np.float32)
    f_str = np.array([str(i % 20) for i in range(TOTAL_DOCS)], dtype=object)
    f_int = np.arange(TOTAL_DOCS, dtype=np.int32) * 10
    f_float = np.arange(TOTAL_DOCS, dtype=np.float32) * 0.5
    _cached_s1 = (vectors, f_str, f_int, f_float)
    return _cached_s1


def _build_s1_space_config():
    fields = [
        {"name": "field_str",   "type": "string"},
        {"name": "field_int",   "type": "integer"},
        {"name": "field_float", "type": "float"},
        {"name": "field_vector", "type": "vector",
         "dimension": EMBEDDING_DIM, "store_type": "MemoryOnly",
         "index": {"name": "gamma", "type": "FLAT", "params": {"metric_type": "L2"}}},
    ]
    indexes = [
        {"name": "idx_composite_s1",  "type": "COMPOSITE", "field_names": ["field_str", "field_int", "field_float"]}
    ]
    return {"name": _STR_INT_FLOAT_NAME, "partition_num": 1, "replica_num": 1, "fields": fields, "indexes": indexes}


def _s1_query(filters, limit=10):
    payload = {"db_name": db_name, "space_name": _STR_INT_FLOAT_NAME,
               "filters": filters, "limit": limit}
    rs = requests.post(router_url + "/document/query",
                       auth=(username, password), json=payload)
    assert rs.status_code == 200, f"s1 query failed: {rs.text}"
    assert rs.json()["code"] == 0, f"s1 code != 0: {rs.json()}"
    return rs.json()["data"]["documents"]


def _s1_batch_upsert():
    vectors, f_str, f_int, f_float = _generate_s1_data()
    docs = []
    for i in range(TOTAL_DOCS):
        docs.append({"_id": str(i), "field_str": str(f_str[i]),
                     "field_int": int(f_int[i]), "field_float": float(f_float[i]),
                     "field_vector": vectors[i].tolist()})
        if len(docs) >= 100 or i == TOTAL_DOCS - 1:
            url = router_url + "/document/upsert?timeout=120000"
            rs = requests.post(url, auth=(username, password),
                              json={"db_name": db_name, "space_name": _STR_INT_FLOAT_NAME,
                                    "documents": docs})
            assert rs.json()["code"] == 0
            docs = []


@pytest.fixture(scope="module")
def composite_s1_space():
    logger.info("Setting up schema-1 (string+int+float) test space...")
    destroy(router_url, db_name, _STR_INT_FLOAT_NAME)
    create_db(router_url, db_name)
    resp = create_space(router_url, db_name, _build_s1_space_config())
    assert resp.json()["code"] == 0, f"s1 create failed: {resp.json()}"
    _s1_batch_upsert()
    yield
    destroy(router_url, db_name, _STR_INT_FLOAT_NAME)


@pytest.fixture(scope="module", autouse=True)
def _request_s1(composite_s1_space):
    pass


class TestStringIntFloat:
    """Schema: [string, int, float]"""

    def test_eq_on_str(self):
        vectors, f_str, f_int, f_float = _generate_s1_data()
        docs = _s1_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["5"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] == "5"]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_not_eq_on_str(self):
        vectors, f_str, f_int, f_float = _generate_s1_data()
        docs = _s1_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "NOT IN", "value": ["5"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] != "5"]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_on_int(self):
        vectors, f_str, f_int, f_float = _generate_s1_data()
        i_val = 1000
        docs = _s1_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": "=", "value": i_val}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_int[i] == i_val]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_eq_all_three(self):
        vectors, f_str, f_int, f_float = _generate_s1_data()
        docs = _s1_query({"operator": "AND", "conditions": [
            {"field": "field_str",   "operator": "IN", "value": ["0", "1"]},
            {"field": "field_int",   "operator": "=", "value": 0},
            {"field": "field_float", "operator": "=", "value": 0.0}]}, limit=TOTAL_DOCS)
        assert len(docs) == 1
        assert int(docs[0]["field_int"]) == 0
        assert float(docs[0]["field_float"]) == 0.0

    def test_noteq_all_three(self):
        vectors, f_str, f_int, f_float = _generate_s1_data()
        docs = _s1_query({"operator": "AND", "conditions": [
            {"field": "field_str",   "operator": "NOT IN", "value": ["0", "1"]},
            {"field": "field_int",   "operator": "=", "value": 0},
            {"field": "field_float", "operator": "=", "value": 0.0}]}, limit=TOTAL_DOCS)
        assert len(docs) == 0

    def test_range_on_float_suffix(self):
        vectors, f_str, f_int, f_float = _generate_s1_data()
        fl_lo, fl_hi = 65.0, 85.0
        docs = _s1_query({"operator": "AND", "conditions": [
            {"field": "field_float", "operator": ">=", "value": fl_lo},
            {"field": "field_float", "operator": "<=", "value": fl_hi}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if fl_lo <= f_float[i] <= fl_hi]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_eq_str_range_int(self):
        """string = v AND int in range"""
        i_lo, i_hi = 100, 199
        docs = _s1_query({"operator": "AND", "conditions": [
            {"field": "field_str",   "operator": "IN", "value": ["5"]},
            {"field": "field_int", "operator": ">=", "value": i_lo},
            {"field": "field_int", "operator": "<=", "value": i_hi}]}, limit=TOTAL_DOCS)
        vectors, f_str, f_int, f_float = _generate_s1_data()
        expected = [i for i in range(TOTAL_DOCS)
                    if f_str[i] == "5" and i_lo <= f_int[i] <= i_hi]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_in_str_range_int(self):
        """string = v AND int in range"""
        str_values = ["5", "10"]
        i_lo, i_hi = 100, 199
        docs = _s1_query({"operator": "AND", "conditions": [
            {"field": "field_str",   "operator": "IN", "value": str_values},
            {"field": "field_int", "operator": ">=", "value": i_lo},
            {"field": "field_int", "operator": "<=", "value": i_hi}]}, limit=TOTAL_DOCS)
        vectors, f_str, f_int, f_float = _generate_s1_data()
        expected = [i for i in range(TOTAL_DOCS)
                    if f_str[i] in str_values and i_lo <= f_int[i] <= i_hi]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_str_range_float(self):
        """string = v AND float in range (unsupported: fallback)"""
        fl_lo, fl_hi = 30.0, 50.0
        docs = _s1_query({"operator": "AND", "conditions": [
            {"field": "field_str",   "operator": "IN", "value": ["5"]},
            {"field": "field_float", "operator": ">=", "value": fl_lo},
            {"field": "field_float", "operator": "<=", "value": fl_hi}]}, limit=TOTAL_DOCS)
        vectors, f_str, f_int, f_float = _generate_s1_data()
        expected = [i for i in range(TOTAL_DOCS)
                    if f_str[i] == "5" and fl_lo <= f_float[i] <= fl_hi]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0


# Schema 2: int + float + string  (string as suffix)
_INT_FLOAT_STR_NAME = "composite_s2_space"

_cached_s2 = None


def _generate_s2_data():
    global _cached_s2
    if _cached_s2 is not None:
        return _cached_s2
    rng = np.random.default_rng(seed=77)
    vectors = rng.random((TOTAL_DOCS, EMBEDDING_DIM), dtype=np.float32)
    f_int = np.arange(TOTAL_DOCS, dtype=np.int32)
    f_float = np.arange(TOTAL_DOCS, dtype=np.float32) * 0.3
    f_str = np.array([str(i % 15) for i in range(TOTAL_DOCS)], dtype=object)
    _cached_s2 = (vectors, f_int, f_float, f_str)
    return _cached_s2


def _build_s2_space_config():
    fields = [
        {"name": "field_int",   "type": "integer"},
        {"name": "field_float", "type": "float"},
        {"name": "field_str",   "type": "string"},
        {"name": "field_vector", "type": "vector",
         "dimension": EMBEDDING_DIM, "store_type": "MemoryOnly",
         "index": {"name": "gamma", "type": "FLAT", "params": {"metric_type": "L2"}}},
    ]
    indexes = [
        {"name": "idx_composite_s2",  "type": "COMPOSITE", "field_names": ["field_int", "field_float", "field_str"]}
    ]
    return {"name": _INT_FLOAT_STR_NAME, "partition_num": 1, "replica_num": 1, "fields": fields, "indexes": indexes}


def _s2_query(filters, limit=10):
    payload = {"db_name": db_name, "space_name": _INT_FLOAT_STR_NAME,
               "filters": filters, "limit": limit}
    rs = requests.post(router_url + "/document/query",
                       auth=(username, password), json=payload)
    assert rs.status_code == 200
    assert rs.json()["code"] == 0
    return rs.json()["data"]["documents"]


def _s2_batch_upsert():
    vectors, f_int, f_float, f_str = _generate_s2_data()
    docs = []
    for i in range(TOTAL_DOCS):
        docs.append({"_id": str(i), "field_int": int(f_int[i]),
                     "field_float": float(f_float[i]), "field_str": str(f_str[i]),
                     "field_vector": vectors[i].tolist()})
        if len(docs) >= 100 or i == TOTAL_DOCS - 1:
            url = router_url + "/document/upsert?timeout=120000"
            rs = requests.post(url, auth=(username, password),
                              json={"db_name": db_name, "space_name": _INT_FLOAT_STR_NAME,
                                    "documents": docs})
            assert rs.json()["code"] == 0
            docs = []


@pytest.fixture(scope="module")
def composite_s2_space():
    logger.info("Setting up schema-2 (int+float+string) test space...")
    destroy(router_url, db_name, _INT_FLOAT_STR_NAME)
    create_db(router_url, db_name)
    resp = create_space(router_url, db_name, _build_s2_space_config())
    assert resp.json()["code"] == 0
    _s2_batch_upsert()
    yield
    destroy(router_url, db_name, _INT_FLOAT_STR_NAME)


@pytest.fixture(scope="module", autouse=True)
def _request_s2(composite_s2_space):
    pass


class TestIntFloatString:
    """Schema: [int, float, string]"""

    def test_eq_on_int(self):
        vectors, f_int, f_float, f_str = _generate_s2_data()
        docs = _s2_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": "=", "value": 500}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_int[i] == 500]
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected
        assert len(docs) > 0

    def test_not_eq_on_int(self):
        vectors, f_int, f_float, f_str = _generate_s2_data()
        docs = _s2_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": "!=", "value": 500}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_int[i] != 500]
        logger.info(f"expected: {expected}")
        assert len(docs) > 0
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_range_on_int_prefix(self):
        vectors, f_int, f_float, f_str = _generate_s2_data()
        i_lo, i_hi = 200, 299
        docs = _s2_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": ">=", "value": i_lo},
            {"field": "field_int", "operator": "<=", "value": i_hi}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if i_lo <= f_int[i] <= i_hi]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected
        assert len(expected) == i_hi - i_lo + 1

    def test_eq_on_int_range_on_float(self):
        vectors, f_int, f_float, f_str = _generate_s2_data()
        docs = _s2_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": "=", "value": 500},
            {"field": "field_float", "operator": ">=", "value": 0.42},
            {"field": "field_float", "operator": "<=", "value": 0.48}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_int[i] == 500 and 0.42 <= f_float[i] <= 0.48]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_int_eq_float_in_str(self):
        vectors, f_int, f_float, f_str = _generate_s2_data()
        docs = _s2_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": "=", "value": 42},
            {"field": "field_float", "operator": "=", "value": 0.42},
            {"field": "field_str", "operator": "IN", "value": ["2", "12"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if f_int[i] == 42 and f_float[i] == 0.42 and f_str[i] in ["2", "12"]]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_eq_int_in_str(self):
        vectors, f_int, f_float, f_str = _generate_s2_data()
        docs = _s2_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": "=", "value": 42},
            {"field": "field_str", "operator": "IN", "value": ["2", "12"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if f_int[i] == 42 and f_str[i] in ["2", "12"]]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_range_int_in_str(self):
        """int range + IN on string suffix"""
        vectors, f_int, f_float, f_str = _generate_s2_data()
        docs = _s2_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": ">=", "value": 100},
            {"field": "field_int", "operator": "<=", "value": 199},
            {"field": "field_str", "operator": "IN", "value": ["5", "9"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if 100 <= f_int[i] <= 199 and f_str[i] in ["5", "9"]]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_eq_float_in_str(self):
        vectors, f_int, f_float, f_str = _generate_s2_data()
        docs = _s2_query({"operator": "AND", "conditions": [
            {"field": "field_float", "operator": "=", "value": 0.42},
            {"field": "field_str", "operator": "IN", "value": ["2", "12"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if f_int[i] == 42 and f_float[i] == 0.42 and f_str[i] in ["2", "12"]]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_eq_float_not_in_str(self):
        vectors, f_int, f_float, f_str = _generate_s2_data()
        docs = _s2_query({"operator": "AND", "conditions": [
            {"field": "field_float", "operator": "=", "value": 0.42},
            {"field": "field_str", "operator": "NOT IN", "value": ["2", "12"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if f_int[i] == 42 and f_float[i] == 0.42 and f_str[i] not in ["2", "12"]]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_range_float_in_str(self):
        vectors, f_int, f_float, f_str = _generate_s2_data()
        docs = _s2_query({"operator": "AND", "conditions": [
            {"field": "field_float", "operator": ">=", "value": 0.42},
            {"field": "field_float", "operator": "<=", "value": 0.48},
            {"field": "field_str", "operator": "IN", "value": ["2", "12"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if 0.42 <= f_float[i] and f_float[i] <= 0.48 and f_str[i] in ["2", "12"]]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

# Schema 3: int + float + string_array  (string_array as suffix)
_INT_FLOAT_STRA_NAME = "composite_s3_space"

_cached_s3 = None


def _generate_s3_data():
    global _cached_s3
    if _cached_s3 is not None:
        return _cached_s3
    rng = np.random.default_rng(seed=55)
    vectors = rng.random((TOTAL_DOCS, EMBEDDING_DIM), dtype=np.float32)
    f_int = np.arange(TOTAL_DOCS, dtype=np.int32)
    f_float = np.arange(TOTAL_DOCS, dtype=np.float32) * 0.7
    f_stra = np.array([[str((i + j) % 10) for j in range(2)] for i in range(TOTAL_DOCS)], dtype=object)
    _cached_s3 = (vectors, f_int, f_float, f_stra)
    return _cached_s3


def _build_s3_space_config():
    fields = [
        {"name": "field_int",   "type": "integer"},
        {"name": "field_float", "type": "float"},
        {"name": "field_stra",  "type": "string_array"},
        {"name": "field_vector", "type": "vector",
         "dimension": EMBEDDING_DIM, "store_type": "MemoryOnly",
         "index": {"name": "gamma", "type": "FLAT", "params": {"metric_type": "L2"}}},
    ]
    indexes = [
        {"name": "idx_composite_s3",  "type": "COMPOSITE", "field_names": ["field_int", "field_float", "field_stra"]}
    ]
    return {"name": _INT_FLOAT_STRA_NAME, "partition_num": 1, "replica_num": 1, "fields": fields, "indexes": indexes}


def _s3_query(filters, limit=10):
    payload = {"db_name": db_name, "space_name": _INT_FLOAT_STRA_NAME,
               "filters": filters, "limit": limit}
    rs = requests.post(router_url + "/document/query",
                       auth=(username, password), json=payload)
    assert rs.status_code == 200
    assert rs.json()["code"] == 0
    return rs.json()["data"]["documents"]


def _s3_batch_upsert():
    vectors, f_int, f_float, f_stra = _generate_s3_data()
    docs = []
    for i in range(TOTAL_DOCS):
        docs.append({"_id": str(i), "field_int": int(f_int[i]),
                     "field_float": float(f_float[i]), "field_stra": f_stra[i].tolist(),
                     "field_vector": vectors[i].tolist()})
        if len(docs) >= 100 or i == TOTAL_DOCS - 1:
            url = router_url + "/document/upsert?timeout=120000"
            rs = requests.post(url, auth=(username, password),
                              json={"db_name": db_name, "space_name": _INT_FLOAT_STRA_NAME,
                                    "documents": docs})
            assert rs.json()["code"] == 0
            docs = []


@pytest.fixture(scope="module")
def composite_s3_space():
    logger.info("Setting up schema-3 (int+float+string_array) test space...")
    destroy(router_url, db_name, _INT_FLOAT_STRA_NAME)
    create_db(router_url, db_name)
    resp = create_space(router_url, db_name, _build_s3_space_config())
    assert resp.json()["code"] == 0
    _s3_batch_upsert()
    yield
    destroy(router_url, db_name, _INT_FLOAT_STRA_NAME)


@pytest.fixture(scope="module", autouse=True)
def _request_s3(composite_s3_space):
    pass


class TestIntFloatStringArray:
    """Schema: [int, float, string_array]"""

    def test_eq_on_int(self):
        vectors, f_int, f_float, f_stra = _generate_s3_data()
        docs = _s3_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": "=", "value": 300}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_int[i] == 300]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_not_eq_on_int(self):
        vectors, f_int, f_float, f_stra = _generate_s3_data()
        docs = _s3_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": "!=", "value": 300}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_int[i] != 300]
        logger.info(f"expected: {expected}")
        assert len(docs) > 0
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_range_on_int(self):
        vectors, f_int, f_float, f_stra = _generate_s3_data()
        docs = _s3_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": ">=", "value": 200},
            {"field": "field_int", "operator": "<=", "value": 299}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if 200 <= f_int[i] <= 299]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected
        assert len(expected) == 299 - 200 + 1

    def test_range_on_float(self):
        vectors, f_int, f_float, f_stra = _generate_s3_data()
        docs = _s3_query({"operator": "AND", "conditions": [
            {"field": "field_float", "operator": ">=", "value": 70.0},
            {"field": "field_float", "operator": "<=", "value": 99.9}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if 70.0 <= f_float[i] <= 99.9]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_eq_int_in_stra(self):
        vectors, f_int, f_float, f_stra = _generate_s3_data()
        stra_vals = ["0", "5"]
        docs = _s3_query({"operator": "AND", "conditions": [
            {"field": "field_int",  "operator": "=", "value": 300},
            {"field": "field_stra", "operator": "IN", "value": stra_vals}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if f_int[i] == 300 and any(t in stra_vals for t in f_stra[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_eq_int_eq_float_in_stra(self):
        vectors, f_int, f_float, f_stra = _generate_s3_data()
        docs = _s3_query({"operator": "AND", "conditions": [
            {"field": "field_int",  "operator": "=", "value": 0},
            {"field": "field_float", "operator": "=", "value": 0.0},
            {"field": "field_stra", "operator": "IN", "value": ["0", "5"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if f_int[i] == 0 and f_float[i] == 0.0 and any(t in ["0", "5"] for t in f_stra[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_range_float_suffix(self):
        vectors, f_int, f_float, f_stra = _generate_s3_data()
        fl_lo, fl_hi = 70.0, 99.9
        docs = _s3_query({"operator": "AND", "conditions": [
            {"field": "field_float", "operator": ">=", "value": fl_lo},
            {"field": "field_float", "operator": "<=", "value": fl_hi}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if fl_lo <= f_float[i] <= fl_hi]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0


# Schema 4: string_array + int + float  (string_array as prefix)
_STRA_IN_FLOAT_NAME = "composite_s4_space"

_cached_s4 = None


def _generate_s4_data():
    global _cached_s4
    if _cached_s4 is not None:
        return _cached_s4
    rng = np.random.default_rng(seed=33)
    vectors = rng.random((TOTAL_DOCS, EMBEDDING_DIM), dtype=np.float32)
    f_stra = np.array([[str((i * 3 + j) % 8) for j in range(3)] for i in range(TOTAL_DOCS)], dtype=object)
    f_int = np.arange(TOTAL_DOCS, dtype=np.int32)
    f_float = np.arange(TOTAL_DOCS, dtype=np.float32) * 1.1
    _cached_s4 = (vectors, f_int, f_stra, f_float)
    return _cached_s4


def _build_s4_space_config():
    fields = [
        {"name": "field_stra",  "type": "string_array"},
        {"name": "field_int",   "type": "integer"},
        {"name": "field_float", "type": "float"},
        {"name": "field_vector", "type": "vector",
         "dimension": EMBEDDING_DIM, "store_type": "MemoryOnly",
         "index": {"name": "gamma", "type": "FLAT", "params": {"metric_type": "L2"}}},
    ]
    indexes = [
        {"name": "idx_composite_s4",  "type": "COMPOSITE", "field_names": ["field_stra", "field_int", "field_float"]}
    ]
    return {"name": _STRA_IN_FLOAT_NAME, "partition_num": 1, "replica_num": 1, "fields": fields, "indexes": indexes}


def _s4_query(filters, limit=10):
    payload = {"db_name": db_name, "space_name": _STRA_IN_FLOAT_NAME,
               "filters": filters, "limit": limit}
    rs = requests.post(router_url + "/document/query",
                       auth=(username, password), json=payload)
    assert rs.status_code == 200
    assert rs.json()["code"] == 0
    return rs.json()["data"]["documents"]


def _s4_batch_upsert():
    vectors, f_int, f_stra, f_float = _generate_s4_data()
    docs = []
    for i in range(TOTAL_DOCS):
        docs.append({"_id": str(i), "field_stra": list(f_stra[i]),
                     "field_int": int(f_int[i]),
                     "field_float": float(f_float[i]),
                     "field_vector": vectors[i].tolist()})
        if len(docs) >= 100 or i == TOTAL_DOCS - 1:
            url = router_url + "/document/upsert?timeout=120000"
            rs = requests.post(url, auth=(username, password),
                              json={"db_name": db_name, "space_name": _STRA_IN_FLOAT_NAME,
                                    "documents": docs})
            assert rs.json()["code"] == 0
            docs = []


@pytest.fixture(scope="module")
def composite_s4_space():
    logger.info("Setting up schema-4 (string_array+int+float) test space...")
    destroy(router_url, db_name, _STRA_IN_FLOAT_NAME)
    create_db(router_url, db_name)
    resp = create_space(router_url, db_name, _build_s4_space_config())
    assert resp.json()["code"] == 0
    _s4_batch_upsert()
    yield
    destroy(router_url, db_name, _STRA_IN_FLOAT_NAME)


@pytest.fixture(scope="module", autouse=True)
def _request_s4(composite_s4_space):
    pass


class TestStringArrayIntFloat:
    """Schema: [string_array, int, float]"""

    def test_eq_on_stra(self):
        vectors, f_int, f_stra, f_float = _generate_s4_data()
        docs = _s4_query({"operator": "AND", "conditions": [
            {"field": "field_stra", "operator": "IN", "value": ["3"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if "3" in f_stra[i]]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_not_eq_on_stra(self):
        vectors, f_int, f_stra, f_float = _generate_s4_data()
        docs = _s4_query({"operator": "AND", "conditions": [
            {"field": "field_stra", "operator": "NOT IN", "value": ["3"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if "3" not in f_stra[i]]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        assert len(docs) > 0
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_in_on_stra(self):
        vectors, f_int, f_stra, f_float = _generate_s4_data()
        docs = _s4_query({"operator": "AND", "conditions": [
            {"field": "field_stra", "operator": "IN", "value": ["3", "5"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if any(t in ["3", "5"] for t in f_stra[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_stra_eq_int(self):
        vectors, f_int, f_stra, f_float = _generate_s4_data()
        int_val = 55
        docs = _s4_query({"operator": "AND", "conditions": [
            {"field": "field_stra", "operator": "IN", "value": ["5"]},
            {"field": "field_int", "operator": "=", "value": int_val}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if any(t in ["5"] for t in f_stra[i]) and f_int[i] == int_val]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_stra_not_eq_int(self):
        vectors, f_int, f_stra, f_float = _generate_s4_data()
        int_val = 55
        docs = _s4_query({"operator": "AND", "conditions": [
            {"field": "field_stra", "operator": "IN", "value": ["5"]},
            {"field": "field_int", "operator": "!=", "value": int_val}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if any(t in ["5"] for t in f_stra[i]) and f_int[i] != int_val]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_eq_stra_range_int(self):
        vectors, f_int, f_stra, f_float = _generate_s4_data()
        int_lo, int_hi = 55, 57
        docs = _s4_query({"operator": "AND", "conditions": [
            {"field": "field_stra",  "operator": "IN", "value": ["5"]},
            {"field": "field_int", "operator": ">=", "value": int_lo},
            {"field": "field_int", "operator": "<=", "value": int_hi}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if any(t in ["5"] for t in f_stra[i]) and int_lo <= f_int[i] <= int_hi]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_in_stra_eq_int(self):
        vectors, f_int, f_stra, f_float = _generate_s4_data()
        int_val = 55
        docs = _s4_query({"operator": "AND", "conditions": [
            {"field": "field_stra", "operator": "IN", "value": ["5", "7"]},
            {"field": "field_int", "operator": "=", "value": int_val}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if any(t in ["5", "7"] for t in f_stra[i]) and f_int[i] == int_val]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_in_stra_range_int(self):
        vectors, f_int, f_stra, f_float = _generate_s4_data()
        int_lo, int_hi = 55, 57
        docs = _s4_query({"operator": "AND", "conditions": [
            {"field": "field_stra", "operator": "IN", "value": ["5", "7"]},
            {"field": "field_int", "operator": ">=", "value": int_lo},
            {"field": "field_int", "operator": "<=", "value": int_hi}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if any(t in ["5", "7"] for t in f_stra[i]) and int_lo <= f_int[i] <= int_hi]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_int(self):
        vectors, f_int, f_stra, f_float = _generate_s4_data()
        int_val = 55
        docs = _s4_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": "=", "value": int_val}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_int[i] == int_val]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_range_int(self):
        vectors, f_int, f_stra, f_float = _generate_s4_data()
        int_lo, int_hi = 55, 57
        docs = _s4_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": ">=", "value": int_lo},
            {"field": "field_int", "operator": "<=", "value": int_hi}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if int_lo <= f_int[i] <= int_hi]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_eq_float(self):
        vectors, f_int, f_stra, f_float = _generate_s4_data()
        float_val = 55.0
        docs = _s4_query({"operator": "AND", "conditions": [
            {"field": "field_float", "operator": "=", "value": float_val}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_float[i] == float_val]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0
    
    def test_range_float(self):
        vectors, f_int, f_stra, f_float = _generate_s4_data()
        float_lo, float_hi = 55.0, 57.0
        docs = _s4_query({"operator": "AND", "conditions": [
            {"field": "field_float", "operator": ">=", "value": float_lo},
            {"field": "field_float", "operator": "<=", "value": float_hi}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if float_lo <= f_float[i] <= float_hi]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0


# Schema 5: int + string + string_array  (string_array as suffix)
_INT_STR_STRA_NAME = "composite_s5_space"

_cached_s5 = None


def _generate_s5_data():
    global _cached_s5
    if _cached_s5 is not None:
        return _cached_s5
    rng = np.random.default_rng(seed=44)
    vectors = rng.random((TOTAL_DOCS, EMBEDDING_DIM), dtype=np.float32)
    f_int = np.arange(TOTAL_DOCS, dtype=np.int32)
    f_str = np.array([str(i % 12) for i in range(TOTAL_DOCS)], dtype=object)
    f_stra = np.array([[str((i + j) % 6) for j in range(2)] for i in range(TOTAL_DOCS)], dtype=object)
    _cached_s5 = (vectors, f_int, f_str, f_stra)
    return _cached_s5


def _build_s5_space_config():
    fields = [
        {"name": "field_int",   "type": "integer"},
        {"name": "field_str",    "type": "string"},
        {"name": "field_stra",  "type": "string_array"},
        {"name": "field_vector", "type": "vector",
         "dimension": EMBEDDING_DIM, "store_type": "MemoryOnly",
         "index": {"name": "gamma", "type": "FLAT", "params": {"metric_type": "L2"}}},
    ]
    indexes = [
        {"name": "idx_composite_s5",  "type": "COMPOSITE", "field_names": ["field_int", "field_str", "field_stra"]}
    ]
    return {"name": _INT_STR_STRA_NAME, "partition_num": 1, "replica_num": 1, "fields": fields, "indexes": indexes}


def _s5_query(filters, limit=10):
    payload = {"db_name": db_name, "space_name": _INT_STR_STRA_NAME,
               "filters": filters, "limit": limit}
    rs = requests.post(router_url + "/document/query",
                       auth=(username, password), json=payload)
    assert rs.status_code == 200
    assert rs.json()["code"] == 0
    return rs.json()["data"]["documents"]


def _s5_batch_upsert():
    vectors, f_int, f_str, f_stra = _generate_s5_data()
    docs = []
    for i in range(TOTAL_DOCS):
        docs.append({"_id": str(i), "field_int": int(f_int[i]),
                     "field_str": str(f_str[i]), "field_stra": list(f_stra[i]),
                     "field_vector": vectors[i].tolist()})
        if len(docs) >= 100 or i == TOTAL_DOCS - 1:
            url = router_url + "/document/upsert?timeout=120000"
            rs = requests.post(url, auth=(username, password),
                              json={"db_name": db_name, "space_name": _INT_STR_STRA_NAME,
                                    "documents": docs})
            assert rs.json()["code"] == 0
            docs = []


@pytest.fixture(scope="module")
def composite_s5_space():
    logger.info("Setting up schema-5 (int+string+string_array) test space...")
    destroy(router_url, db_name, _INT_STR_STRA_NAME)
    create_db(router_url, db_name)
    resp = create_space(router_url, db_name, _build_s5_space_config())
    assert resp.json()["code"] == 0
    _s5_batch_upsert()
    yield
    destroy(router_url, db_name, _INT_STR_STRA_NAME)


@pytest.fixture(scope="module", autouse=True)
def _request_s5(composite_s5_space):
    pass


class TestIntStringStringArray:
    """Schema: [int, string, string_array]"""

    def test_eq_on_int(self):
        vectors, f_int, f_str, f_stra = _generate_s5_data()
        docs = _s5_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": "=", "value": 250}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_int[i] == 250]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_not_eq_on_int(self):
        vectors, f_int, f_str, f_stra = _generate_s5_data()
        docs = _s5_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": "!=", "value": 250}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_int[i] != 250]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected
        assert len(docs) > 0

    def test_range_int(self):
        vectors, f_int, f_str, f_stra = _generate_s5_data()
        docs = _s5_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": ">=", "value": 250},
            {"field": "field_int", "operator": "<=", "value": 252}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if 250 <= f_int[i] <= 252]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_int_eq_str(self):
        vectors, f_int, f_str, f_stra = _generate_s5_data()
        docs = _s5_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": "=", "value": 360},
            {"field": "field_str", "operator": "IN", "value": ["0"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if f_int[i] == 360 and any(t in ["0"] for t in f_str[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_not_eq_int_eq_str(self):
        vectors, f_int, f_str, f_stra = _generate_s5_data()
        docs = _s5_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": "!=", "value": 360},
            {"field": "field_str", "operator": "IN", "value": ["0"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if f_int[i] != 360 and any(t in ["0"] for t in f_str[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_eq_int_in_str(self):
        vectors, f_int, f_str, f_stra = _generate_s5_data()
        docs = _s5_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": "=", "value": 360},
            {"field": "field_str", "operator": "IN", "value": ["0"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if f_int[i] == 360 and any(t in ["0"] for t in f_str[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_int_eq_stra(self):
        vectors, f_int, f_str, f_stra = _generate_s5_data()
        docs = _s5_query({"operator": "AND", "conditions": [
            {"field": "field_int",  "operator": "=", "value": 300},
            {"field": "field_stra", "operator": "IN", "value": ["0"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if f_int[i] == 300 and any(t in ["0"] for t in f_stra[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_eq_int_eq_str_eq_stra(self):
        vectors, f_int, f_str, f_stra = _generate_s5_data()
        docs = _s5_query({"operator": "AND", "conditions": [
            {"field": "field_int",  "operator": "=", "value": 300},
            {"field": "field_str", "operator": "IN", "value": ["0"]},
            {"field": "field_stra", "operator": "IN", "value": ["0"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if f_int[i] == 300 and f_str[i] == "0" and any(t in ["0"] for t in f_stra[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_range_int_eq_str_eq_stra(self):
        vectors, f_int, f_str, f_stra = _generate_s5_data()
        docs = _s5_query({"operator": "AND", "conditions": [
            {"field": "field_int",  "operator": ">=", "value": 300},
            {"field": "field_int",  "operator": "<=", "value": 302},
            {"field": "field_str", "operator": "IN", "value": ["0"]},
            {"field": "field_stra", "operator": "IN", "value": ["0"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if 300 <= f_int[i] <= 302 and f_str[i] == "0" and any(t in ["0"] for t in f_stra[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_range_int_eq_str_not_eq_stra(self):
        vectors, f_int, f_str, f_stra = _generate_s5_data()
        docs = _s5_query({"operator": "AND", "conditions": [
            {"field": "field_int",  "operator": ">=", "value": 300},
            {"field": "field_int",  "operator": "<=", "value": 302},
            {"field": "field_str", "operator": "IN", "value": ["0"]},
            {"field": "field_stra", "operator": "NOT IN", "value": ["0"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if 300 <= f_int[i] <= 302 and f_str[i] == "0" and "0" not in f_stra[i]]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_range_int_in_str_in_stra(self):
        vectors, f_int, f_str, f_stra = _generate_s5_data()
        docs = _s5_query({"operator": "AND", "conditions": [
            {"field": "field_int",  "operator": ">=", "value": 300},
            {"field": "field_int",  "operator": "<=", "value": 302},
            {"field": "field_str", "operator": "IN", "value": ["0", "1"]},
            {"field": "field_stra", "operator": "IN", "value": ["0", "1"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if 300 <= f_int[i] <= 302 and any(t in ["0", "1"] for t in f_str[i]) and any(t in ["0", "1"] for t in f_stra[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_eq_int_in_stra(self):
        vectors, f_int, f_str, f_stra = _generate_s5_data()
        stra_vals = ["0", "3"]
        docs = _s5_query({"operator": "AND", "conditions": [
            {"field": "field_int",  "operator": "=", "value": 300},
            {"field": "field_stra", "operator": "IN", "value": stra_vals}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if f_int[i] == 300 and any(t in stra_vals for t in f_stra[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_eq_str(self):
        vectors, f_int, f_str, f_stra = _generate_s5_data()
        docs = _s5_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["0"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if any(t in ["0"] for t in f_str[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_in_str(self):
        vectors, f_int, f_str, f_stra = _generate_s5_data()
        docs = _s5_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["0", "1"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if any(t in ["0", "1"] for t in f_str[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_eq_stra(self):
        vectors, f_int, f_str, f_stra = _generate_s5_data()
        docs = _s5_query({"operator": "AND", "conditions": [
            {"field": "field_stra", "operator": "IN", "value": ["0"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if any(t in ["0"] for t in f_stra[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_in_stra(self):
        vectors, f_int, f_str, f_stra = _generate_s5_data()
        docs = _s5_query({"operator": "AND", "conditions": [
            {"field": "field_stra", "operator": "IN", "value": ["0", "1"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if any(t in ["0", "1"] for t in f_stra[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_eq_str_eq_stra(self):
        vectors, f_int, f_str, f_stra = _generate_s5_data()
        docs = _s5_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["0"]},
            {"field": "field_stra", "operator": "IN", "value": ["0"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if f_str[i] == "0" and any(t in ["0"] for t in f_stra[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0


# Schema 6: string + string_array + int  (int as suffix)
_STR_STRA_INT_NAME = "composite_s6_space"

_cached_s6 = None


def _generate_s6_data():
    global _cached_s6
    if _cached_s6 is not None:
        return _cached_s6
    rng = np.random.default_rng(seed=66)
    vectors = rng.random((TOTAL_DOCS, EMBEDDING_DIM), dtype=np.float32)
    f_str = np.array([str(i % 18) for i in range(TOTAL_DOCS)], dtype=object)
    f_stra = np.array([[str((i * 2 + j) % 7) for j in range(2)] for i in range(TOTAL_DOCS)], dtype=object)
    f_int = np.arange(TOTAL_DOCS, dtype=np.int32)
    _cached_s6 = (vectors, f_str, f_stra, f_int)
    return _cached_s6


def _build_s6_space_config():
    fields = [
        {"name": "field_str",   "type": "string"},
        {"name": "field_stra",  "type": "string_array"},
        {"name": "field_int",   "type": "integer"},
        {"name": "field_vector", "type": "vector",
         "dimension": EMBEDDING_DIM, "store_type": "MemoryOnly",
         "index": {"name": "gamma", "type": "FLAT", "params": {"metric_type": "L2"}}},
    ]
    indexes = [
        {"name": "idx_composite_s6",  "type": "COMPOSITE", "field_names": ["field_str", "field_stra", "field_int"]}
    ]
    return {"name": _STR_STRA_INT_NAME, "partition_num": 1, "replica_num": 1, "fields": fields, "indexes": indexes}


def _s6_query(filters, limit=10):
    payload = {"db_name": db_name, "space_name": _STR_STRA_INT_NAME,
               "filters": filters, "limit": limit}
    rs = requests.post(router_url + "/document/query",
                       auth=(username, password), json=payload)
    assert rs.status_code == 200
    assert rs.json()["code"] == 0
    return rs.json()["data"]["documents"]


def _s6_batch_upsert():
    vectors, f_str, f_stra, f_int = _generate_s6_data()
    docs = []
    for i in range(TOTAL_DOCS):
        docs.append({"_id": str(i), "field_str": str(f_str[i]),
                     "field_stra": list(f_stra[i]), "field_int": int(f_int[i]),
                     "field_vector": vectors[i].tolist()})
        if len(docs) >= 100 or i == TOTAL_DOCS - 1:
            url = router_url + "/document/upsert?timeout=120000"
            rs = requests.post(url, auth=(username, password),
                              json={"db_name": db_name, "space_name": _STR_STRA_INT_NAME,
                                    "documents": docs})
            assert rs.json()["code"] == 0
            docs = []


@pytest.fixture(scope="module")
def composite_s6_space():
    logger.info("Setting up schema-6 (string+string_array+int) test space...")
    destroy(router_url, db_name, _STR_STRA_INT_NAME)
    create_db(router_url, db_name)
    resp = create_space(router_url, db_name, _build_s6_space_config())
    assert resp.json()["code"] == 0
    _s6_batch_upsert()
    yield
    destroy(router_url, db_name, _STR_STRA_INT_NAME)


@pytest.fixture(scope="module", autouse=True)
def _request_s6(composite_s6_space):
    pass


class TestStringStringArrayInt:
    """Schema: [string, string_array, int]"""

    def test_eq_on_str(self):
        vectors, f_str, f_stra, f_int = _generate_s6_data()
        docs = _s6_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] == "7"]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected
        assert len(docs) == len(expected)

    def test_not_eq_on_str(self):
        vectors, f_str, f_stra, f_int = _generate_s6_data()
        docs = _s6_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "NOT IN", "value": ["7"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] != "7"]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected
        assert len(docs) > 0

    def test_in_on_str(self):
        vectors, f_str, f_stra, f_int = _generate_s6_data()
        docs = _s6_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7", "8"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] in ["7", "8"]]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_on_str_eq_stra(self):
        vectors, f_str, f_stra, f_int = _generate_s6_data()
        docs = _s6_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7"]},
            {"field": "field_stra", "operator": "IN", "value": ["3"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] == "7" and "3" in f_stra[i]]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected
        assert len(docs) == len(expected)

    def test_eq_on_str_not_eq_stra(self):
        vectors, f_str, f_stra, f_int = _generate_s6_data()
        docs = _s6_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7"]},
            {"field": "field_stra", "operator": "NOT IN", "value": ["3"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] == "7" and "3" not in f_stra[i]]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_eq_on_str_in_stra(self):
        vectors, f_str, f_stra, f_int = _generate_s6_data()
        docs = _s6_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7"]},
            {"field": "field_stra", "operator": "IN", "value": ["3", "4"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] == "7" and any(t in ["3", "4"] for t in f_stra[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected
        assert len(docs) == len(expected)

    def test_in_on_str_eq_stra(self):
        vectors, f_str, f_stra, f_int = _generate_s6_data()
        docs = _s6_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7", "8"]},
            {"field": "field_stra", "operator": "IN", "value": ["3"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] in ["7", "8"] and "3" in f_stra[i]]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_in_on_str_in_stra(self):
        vectors, f_str, f_stra, f_int = _generate_s6_data()
        docs = _s6_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7", "8"]},
            {"field": "field_stra", "operator": "IN", "value": ["3", "4"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] in ["7", "8"] and any(t in ["3", "4"] for t in f_stra[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_on_str_eq_stra_eq_int(self):
        vectors, f_str, f_stra, f_int = _generate_s6_data()
        docs = _s6_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7"]},
            {"field": "field_stra", "operator": "IN", "value": ["3"]},
            {"field": "field_int", "operator": "=", "value": 6}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] == "7" and any(t in ["3"] for t in f_stra[i]) and f_int[i] == 6]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_on_str_eq_stra_range_int(self):
        vectors, f_str, f_stra, f_int = _generate_s6_data()
        docs = _s6_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7"]},
            {"field": "field_stra", "operator": "IN", "value": ["3"]},
            {"field": "field_int", "operator": ">=", "value": 6},
            {"field": "field_int", "operator": "<=", "value": 60}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] == "7" and any(t in ["3"] for t in f_stra[i]) and 6 <= f_int[i] <= 60]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_in_on_str_eq_stra_range_int(self):
        vectors, f_str, f_stra, f_int = _generate_s6_data()
        docs = _s6_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7", "8"]},
            {"field": "field_stra", "operator": "IN", "value": ["3"]},
            {"field": "field_int", "operator": ">=", "value": 6},
            {"field": "field_int", "operator": "<=", "value": 60}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] in ["7", "8"] and "3" in f_stra[i] and 6 <= f_int[i] <= 60]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected


    def test_in_on_str_in_stra_range_int(self):
        vectors, f_str, f_stra, f_int = _generate_s6_data()
        docs = _s6_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7", "8"]},
            {"field": "field_stra", "operator": "IN", "value": ["3", "4"]},
            {"field": "field_int", "operator": ">=", "value": 6},
            {"field": "field_int", "operator": "<=", "value": 60}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] in ["7", "8"] and any(t in ["3", "4"] for t in f_stra[i]) and 6 <= f_int[i] <= 60]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_on_stra(self):
        vectors, f_str, f_stra, f_int = _generate_s6_data()
        docs = _s6_query({"operator": "AND", "conditions": [
            {"field": "field_stra", "operator": "IN", "value": ["3"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if any(t in ["3"] for t in f_stra[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_in_on_stra(self):
        vectors, f_str, f_stra, f_int = _generate_s6_data()
        docs = _s6_query({"operator": "AND", "conditions": [
            {"field": "field_stra", "operator": "IN", "value": ["3", "4"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if any(t in ["3", "4"] for t in f_stra[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_in_stra_eq_int(self):
        vectors, f_str, f_stra, f_int = _generate_s6_data()
        docs = _s6_query({"operator": "AND", "conditions": [
            {"field": "field_stra", "operator": "IN", "value": ["3", "4"]},
            {"field": "field_int",  "operator": "=", "value": 6}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if any(t in ["3", "4"] for t in f_stra[i]) and f_int[i] == 6]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_in_stra_range_int(self):
        vectors, f_str, f_stra, f_int = _generate_s6_data()
        docs = _s6_query({"operator": "AND", "conditions": [
            {"field": "field_stra",  "operator": "IN", "value": ["4", "5"]},
            {"field": "field_int", "operator": ">=", "value": 6},
            {"field": "field_int", "operator": "<=", "value": 60}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if any(t in ["4", "5"] for t in f_stra[i]) and 6 <= f_int[i] <= 60]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0


# Schema 7: string + string_array + int  (int as suffix) with other scalar index
_STR_STRA_INT_NAME = "composite_s7_space"

_cached_s7 = None


def _generate_s7_data():
    global _cached_s7
    if _cached_s7 is not None:
        return _cached_s7
    rng = np.random.default_rng(seed=66)
    vectors = rng.random((TOTAL_DOCS, EMBEDDING_DIM), dtype=np.float32)
    f_str = np.array([str(i % 18) for i in range(TOTAL_DOCS)], dtype=object)
    f_stra = np.array([[str((i * 2 + j) % 7) for j in range(2)] for i in range(TOTAL_DOCS)], dtype=object)
    f_int = np.arange(TOTAL_DOCS, dtype=np.int32)
    _cached_s7 = (vectors, f_str, f_stra, f_int)
    return _cached_s7


def _build_s7_space_config():
    fields = [
        {"name": "field_str",   "type": "string", "index": {"name": "idx_str", "type": "INVERTED"}},
        {"name": "field_stra",  "type": "string_array"},
        {"name": "field_int",   "type": "integer", "index": {"name": "idx_int", "type": "INVERTED"}},
        {"name": "field_vector", "type": "vector",
         "dimension": EMBEDDING_DIM, "store_type": "MemoryOnly",
         "index": {"name": "gamma", "type": "FLAT", "params": {"metric_type": "L2"}}},
    ]
    indexes = [
        {"name": "idx_composite_s7",  "type": "COMPOSITE", "field_names": ["field_str", "field_stra", "field_int"]}
    ]
    return {"name": _STR_STRA_INT_NAME, "partition_num": 1, "replica_num": 1, "fields": fields, "indexes": indexes}

def _s7_query(filters, limit=10):
    payload = {"db_name": db_name, "space_name": _STR_STRA_INT_NAME,
               "filters": filters, "limit": limit}
    rs = requests.post(router_url + "/document/query",
                       auth=(username, password), json=payload)
    assert rs.status_code == 200
    assert rs.json()["code"] == 0
    return rs.json()["data"]["documents"]


def _s7_batch_upsert():
    vectors, f_str, f_stra, f_int = _generate_s6_data()
    docs = []
    for i in range(TOTAL_DOCS):
        docs.append({"_id": str(i), "field_str": str(f_str[i]),
                     "field_stra": list(f_stra[i]), "field_int": int(f_int[i]),
                     "field_vector": vectors[i].tolist()})
        if len(docs) >= 100 or i == TOTAL_DOCS - 1:
            url = router_url + "/document/upsert?timeout=120000"
            rs = requests.post(url, auth=(username, password),
                              json={"db_name": db_name, "space_name": _STR_STRA_INT_NAME,
                                    "documents": docs})
            assert rs.json()["code"] == 0
            docs = []


@pytest.fixture(scope="module")
def composite_s7_space():
    logger.info("Setting up schema-7 (string+string_array+int) test space...")
    destroy(router_url, db_name, _STR_STRA_INT_NAME)
    create_db(router_url, db_name)
    resp = create_space(router_url, db_name, _build_s7_space_config())
    assert resp.json()["code"] == 0
    _s7_batch_upsert()
    yield
    destroy(router_url, db_name, _STR_STRA_INT_NAME)


@pytest.fixture(scope="module", autouse=True)
def _request_s7(composite_s7_space):
    pass


class TestStringStringArrayIntWithOtherScalarIndex:
    """Schema: [string, string_array, int]"""

    def test_eq_on_str(self):
        vectors, f_str, f_stra, f_int = _generate_s7_data()
        docs = _s7_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] == "7"]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected
        assert len(docs) == len(expected)

    def test_not_eq_on_str(self):
        vectors, f_str, f_stra, f_int = _generate_s7_data()
        docs = _s7_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "NOT IN", "value": ["7"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] != "7"]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected
        assert len(docs) > 0

    def test_in_on_str(self):
        vectors, f_str, f_stra, f_int = _generate_s7_data()
        docs = _s7_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7", "8"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] in ["7", "8"]]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_on_str_eq_stra(self):
        vectors, f_str, f_stra, f_int = _generate_s7_data()
        docs = _s7_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7"]},
            {"field": "field_stra", "operator": "IN", "value": ["3"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] == "7" and "3" in f_stra[i]]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected
        assert len(docs) == len(expected)

    def test_eq_on_str_not_eq_stra(self):
        vectors, f_str, f_stra, f_int = _generate_s7_data()
        docs = _s7_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7"]},
            {"field": "field_stra", "operator": "NOT IN", "value": ["3"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] == "7" and "3" not in f_stra[i]]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_eq_on_str_in_stra(self):
        vectors, f_str, f_stra, f_int = _generate_s7_data()
        docs = _s7_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7"]},
            {"field": "field_stra", "operator": "IN", "value": ["3", "4"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] == "7" and any(t in ["3", "4"] for t in f_stra[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected
        assert len(docs) == len(expected)

    def test_in_on_str_eq_stra(self):
        vectors, f_str, f_stra, f_int = _generate_s7_data()
        docs = _s7_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7", "8"]},
            {"field": "field_stra", "operator": "IN", "value": ["3"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] in ["7", "8"] and "3" in f_stra[i]]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_in_on_str_in_stra(self):
        vectors, f_str, f_stra, f_int = _generate_s7_data()
        docs = _s7_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7", "8"]},
            {"field": "field_stra", "operator": "IN", "value": ["3", "4"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] in ["7", "8"] and any(t in ["3", "4"] for t in f_stra[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_on_str_eq_stra_eq_int(self):
        vectors, f_str, f_stra, f_int = _generate_s7_data()
        docs = _s7_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7"]},
            {"field": "field_stra", "operator": "IN", "value": ["3"]},
            {"field": "field_int", "operator": "=", "value": 6}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] == "7" and any(t in ["3"] for t in f_stra[i]) and f_int[i] == 6]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_on_str_eq_stra_range_int(self):
        vectors, f_str, f_stra, f_int = _generate_s7_data()
        docs = _s7_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7"]},
            {"field": "field_stra", "operator": "IN", "value": ["3"]},
            {"field": "field_int", "operator": ">=", "value": 6},
            {"field": "field_int", "operator": "<=", "value": 60}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] == "7" and any(t in ["3"] for t in f_stra[i]) and 6 <= f_int[i] <= 60]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_in_on_str_eq_stra_range_int(self):
        vectors, f_str, f_stra, f_int = _generate_s7_data()
        docs = _s7_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7", "8"]},
            {"field": "field_stra", "operator": "IN", "value": ["3"]},
            {"field": "field_int", "operator": ">=", "value": 6},
            {"field": "field_int", "operator": "<=", "value": 60}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] in ["7", "8"] and "3" in f_stra[i] and 6 <= f_int[i] <= 60]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected


    def test_in_on_str_in_stra_range_int(self):
        vectors, f_str, f_stra, f_int = _generate_s7_data()
        docs = _s7_query({"operator": "AND", "conditions": [
            {"field": "field_str", "operator": "IN", "value": ["7", "8"]},
            {"field": "field_stra", "operator": "IN", "value": ["3", "4"]},
            {"field": "field_int", "operator": ">=", "value": 6},
            {"field": "field_int", "operator": "<=", "value": 60}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_str[i] in ["7", "8"] and any(t in ["3", "4"] for t in f_stra[i]) and 6 <= f_int[i] <= 60]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_on_stra(self):
        vectors, f_str, f_stra, f_int = _generate_s7_data()
        docs = _s7_query({"operator": "AND", "conditions": [
            {"field": "field_stra", "operator": "IN", "value": ["3"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if any(t in ["3"] for t in f_stra[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_in_on_stra(self):
        vectors, f_str, f_stra, f_int = _generate_s7_data()
        docs = _s7_query({"operator": "AND", "conditions": [
            {"field": "field_stra", "operator": "IN", "value": ["3", "4"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if any(t in ["3", "4"] for t in f_stra[i])]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_in_stra_eq_int(self):
        vectors, f_str, f_stra, f_int = _generate_s7_data()
        docs = _s7_query({"operator": "AND", "conditions": [
            {"field": "field_stra", "operator": "IN", "value": ["3", "4"]},
            {"field": "field_int",  "operator": "=", "value": 6}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if any(t in ["3", "4"] for t in f_stra[i]) and f_int[i] == 6]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_in_stra_range_int(self):
        vectors, f_str, f_stra, f_int = _generate_s7_data()
        docs = _s7_query({"operator": "AND", "conditions": [
            {"field": "field_stra",  "operator": "IN", "value": ["4", "5"]},
            {"field": "field_int", "operator": ">=", "value": 6},
            {"field": "field_int", "operator": "<=", "value": 60}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if any(t in ["4", "5"] for t in f_stra[i]) and 6 <= f_int[i] <= 60]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_eq_int(self):
        vectors, f_str, f_stra, f_int = _generate_s7_data()
        docs = _s7_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": "=", "value": 6}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS) if f_int[i] == 6]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_range_int(self):
        vectors, f_str, f_stra, f_int = _generate_s7_data()
        docs = _s7_query({"operator": "AND", "conditions": [
            {"field": "field_int", "operator": ">=", "value": 6},
            {"field": "field_int", "operator": "<=", "value": 60}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if 6 <= f_int[i] <= 60]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected)
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected


# Schema 8: two overlapping composite indexes [A,B] + [B,C] + extra D field
# Tests: composite index merge (larger one wins), field-order independence (B AND A == A AND B),
# and hybrid query (A AND B AND C AND D -> [A,B] + [B,C] for ABC, D uses scalar index).
_MULTI_COMPOSITE_SPACE_NAME = "composite_s8_space"

_cached_s8 = None


def _generate_s8_data():
    global _cached_s8
    if _cached_s8 is not None:
        return _cached_s8
    rng = np.random.default_rng(seed=99)
    vectors = rng.random((TOTAL_DOCS, EMBEDDING_DIM), dtype=np.float32)
    # field_A: 0..999
    f_a = np.arange(TOTAL_DOCS, dtype=np.int32)
    # field_B: field_A * 10  (unique for each doc)
    f_b = (f_a * 10).astype(np.int32)
    # field_C: sequential 0..19 (cardinality 20)
    f_c = np.array([str(i % 20) for i in range(TOTAL_DOCS)], dtype=object)
    # field_D: sequential 0..999 (used for scalar-only query)
    f_d = np.arange(TOTAL_DOCS, dtype=np.int32)
    _cached_s8 = (vectors, f_a, f_b, f_c, f_d)
    return _cached_s8


def _build_s8_space_config():
    fields = [
        {"name": "field_A",  "type": "integer"},
        {"name": "field_B",  "type": "integer"},
        {"name": "field_C",  "type": "string"},
        {"name": "field_D",  "type": "integer", "index": {"name": "idx_D", "type": "INVERTED"}},
        {"name": "field_vector", "type": "vector",
         "dimension": EMBEDDING_DIM, "store_type": "MemoryOnly",
         "index": {"name": "gamma", "type": "FLAT", "params": {"metric_type": "L2"}}},
    ]
    indexes = [
        # Composite index 1: [A, B]
        {"name": "idx_composite_AB", "type": "COMPOSITE", "field_names": ["field_A", "field_B"]},
        # Composite index 2: [B, C]  (overlaps with index 1 on field_B)
        {"name": "idx_composite_BC", "type": "COMPOSITE", "field_names": ["field_B", "field_C"]},
    ]
    return {
        "name": _MULTI_COMPOSITE_SPACE_NAME,
        "partition_num": 1,
        "replica_num": 1,
        "fields": fields,
        "indexes": indexes,
    }


def _s8_query(filters, limit=10):
    payload = {"db_name": db_name, "space_name": _MULTI_COMPOSITE_SPACE_NAME,
               "filters": filters, "limit": limit}
    rs = requests.post(router_url + "/document/query",
                       auth=(username, password), json=payload)
    assert rs.status_code == 200, f"s8 query failed: {rs.text}"
    assert rs.json()["code"] == 0, f"s8 query code != 0: {rs.json()}"
    return rs.json()["data"]["documents"]


def _s8_batch_upsert():
    vectors, f_a, f_b, f_c, f_d = _generate_s8_data()
    docs = []
    for i in range(TOTAL_DOCS):
        docs.append({
            "_id": str(i),
            "field_A": int(f_a[i]),
            "field_B": int(f_b[i]),
            "field_C": str(f_c[i]),
            "field_D": int(f_d[i]),
            "field_vector": vectors[i].tolist(),
        })
        if len(docs) >= 100 or i == TOTAL_DOCS - 1:
            url = router_url + "/document/upsert?timeout=120000"
            rs = requests.post(url, auth=(username, password),
                              json={"db_name": db_name, "space_name": _MULTI_COMPOSITE_SPACE_NAME,
                                    "documents": docs})
            assert rs.json()["code"] == 0
            docs = []


@pytest.fixture(scope="module")
def composite_s8_space():
    logger.info("Setting up schema-8 (two overlapping composite indexes [A,B] + [B,C]) test space...")
    destroy(router_url, db_name, _MULTI_COMPOSITE_SPACE_NAME)
    create_db(router_url, db_name)
    resp = create_space(router_url, db_name, _build_s8_space_config())
    assert resp.json()["code"] == 0, f"create_space s8 failed: {resp.json()}"
    _s8_batch_upsert()
    yield
    destroy(router_url, db_name, _MULTI_COMPOSITE_SPACE_NAME)


@pytest.fixture(scope="module", autouse=True)
def _request_s8(composite_s8_space):
    pass


class TestMultiCompositeIndexes:
    """Schema: [A:int, B:int, C:string, D:int] with composite indexes [A,B] and [B,C].

    Merge logic tests:
      - Query A AND B  -> use [A,B] (neither [A,B] nor [B,C] fully covers more fields)
      - Query B AND C  -> use [B,C]
      - Query A AND B AND C -> merge: [A,B] + [B,C] (both needed to cover all 3 fields)
      - Query B AND A  -> same as A AND B (field order independence after sort)
      - Query A AND B AND D -> [A,B] for A,B; D uses scalar idx_D
      - Query A AND B AND C AND D -> [A,B] + [B,C] for ABC; D uses scalar idx_D
    """

    def test_eq_A_B(self):
        """Query A AND B: use composite [A,B]."""
        vectors, f_a, f_b, f_c, f_d = _generate_s8_data()
        docs = _s8_query({"operator": "AND", "conditions": [
            {"field": "field_A", "operator": "=", "value": 100},
            {"field": "field_B", "operator": "=", "value": 1000}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if f_a[i] == 100 and f_b[i] == 1000]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected), f"got {len(docs)}, expected {len(expected)}"
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_B_A(self):
        """Query B AND A (reversed order): same result as A AND B.

        Verifies field-order independence after sorting filters by index field order.
        """
        vectors, f_a, f_b, f_c, f_d = _generate_s8_data()
        docs = _s8_query({"operator": "AND", "conditions": [
            {"field": "field_B", "operator": "=", "value": 500},
            {"field": "field_A", "operator": "=", "value": 50}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if f_a[i] == 50 and f_b[i] == 500]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected), f"got {len(docs)}, expected {len(expected)}"
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_B_C(self):
        """Query B AND C: use composite [B,C]."""
        vectors, f_a, f_b, f_c, f_d = _generate_s8_data()
        docs = _s8_query({"operator": "AND", "conditions": [
            {"field": "field_B", "operator": "=", "value": 300},
            {"field": "field_C", "operator": "IN", "value": ["0"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if f_b[i] == 300 and f_c[i] == "0"]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected), f"got {len(docs)}, expected {len(expected)}"
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_A_B_C(self):
        """Query A AND B AND C: merge [A,B] + [B,C] -> both execute, intersection gives result.

        Neither composite fully covers all 3 fields, so both are needed.
        """
        vectors, f_a, f_b, f_c, f_d = _generate_s8_data()
        docs = _s8_query({"operator": "AND", "conditions": [
            {"field": "field_A", "operator": "=", "value": 200},
            {"field": "field_B", "operator": "=", "value": 2000},
            {"field": "field_C", "operator": "IN", "value": ["0"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if f_a[i] == 200 and f_b[i] == 2000 and f_c[i] == "0"]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected), f"got {len(docs)}, expected {len(expected)}"
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_A_B_D(self):
        """Query A AND B AND D: [A,B] for A,B; D uses scalar index idx_D.

        D is not in any composite index, so it falls through to the scalar path.
        """
        vectors, f_a, f_b, f_c, f_d = _generate_s8_data()
        docs = _s8_query({"operator": "AND", "conditions": [
            {"field": "field_A", "operator": "=", "value": 150},
            {"field": "field_B", "operator": "=", "value": 1500},
            {"field": "field_D", "operator": "=", "value": 150}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if f_a[i] == 150 and f_b[i] == 1500 and f_d[i] == 150]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected), f"got {len(docs)}, expected {len(expected)}"
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_eq_A_B_C_D(self):
        """Query A AND B AND C AND D: [A,B] + [B,C] for ABC; D uses scalar idx_D.

        Full hybrid: overlapping composites + scalar index.
        """
        vectors, f_a, f_b, f_c, f_d = _generate_s8_data()
        docs = _s8_query({"operator": "AND", "conditions": [
            {"field": "field_A", "operator": "=", "value": 250},
            {"field": "field_B", "operator": "=", "value": 2500},
            {"field": "field_C", "operator": "IN", "value": ["10"]},
            {"field": "field_D", "operator": "=", "value": 250}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if f_a[i] == 250 and f_b[i] == 2500
                    and f_c[i] == "10" and f_d[i] == 250]
        logger.info(f"expected: {expected}")
        assert len(docs) == len(expected), f"got {len(docs)}, expected {len(expected)}"
        doc_ids = sorted(int(d["_id"]) for d in docs)
        assert doc_ids == expected

    def test_range_A_B(self):
        """Query range on A and B: [A,B] composite handles Range on both fields."""
        vectors, f_a, f_b, f_c, f_d = _generate_s8_data()
        docs = _s8_query({"operator": "AND", "conditions": [
            {"field": "field_A", "operator": ">=", "value": 100},
            {"field": "field_A", "operator": "<=", "value": 105},
            {"field": "field_B", "operator": ">=", "value": 1000},
            {"field": "field_B", "operator": "<=", "value": 1050}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if 100 <= f_a[i] <= 105 and 1000 <= f_b[i] <= 1050]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0

    def test_range_A_in_B_C(self):
        """Query range on A, IN on B and C: [A,B] + [B,C] both execute, intersection."""
        vectors, f_a, f_b, f_c, f_d = _generate_s8_data()
        docs = _s8_query({"operator": "AND", "conditions": [
            {"field": "field_A", "operator": ">=", "value": 50},
            {"field": "field_A", "operator": "<=", "value": 55},
            {"field": "field_B", "operator": "=", "value": 500},
            {"field": "field_C", "operator": "IN", "value": ["5", "10"]}]}, limit=TOTAL_DOCS)
        expected = [i for i in range(TOTAL_DOCS)
                    if 50 <= f_a[i] <= 55
                    and f_b[i] == 500
                    and f_c[i] in ["5", "10"]]
        logger.info(f"expected: {expected}")
        assert len(docs) == 0