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

import pytest
import numpy as np
import time
import json
import requests
from utils.vearch_utils import *
from utils.data_utils import *

__description__ = """test case for scalar index benchmark with different cardinalities and index types"""

SCALAR_INDEX_TYPES = [
    # "SCALAR",
    "INVERTED",
    "BITMAP",
]

# Cardinality configs: cardinality_value -> field_name
CARDINALITY_CONFIGS = {
    1: "field_cardinality_1",
    2: "field_cardinality_2",
    10: "field_cardinality_10",
    50: "field_cardinality_50",
}

# Module-scoped data cache (shared across all scalar_index_type variants)
_cached_data: dict = {}


def _generate_cached_data(total_docs: int, embedding_dim: int = 128):
    """Generate and cache data once per module session."""
    cache_key = (total_docs, embedding_dim)
    if cache_key not in _cached_data:
        logger.info(f"Generating data for {total_docs} docs x {embedding_dim} dim (cached)...")
        rng = np.random.default_rng(seed=42)
        vectors = rng.random((total_docs, embedding_dim), dtype=np.float32)
        cardinality_map = {}
        for cardinality, field_name in CARDINALITY_CONFIGS.items():
            values = rng.integers(0, cardinality, size=total_docs, dtype=np.int32)
            cardinality_map[field_name] = values
        _cached_data[cache_key] = (vectors, cardinality_map)
        logger.info(f"Data cached: vectors {vectors.shape}, "
                     f"cardinalities {[f for f in cardinality_map]}")
    return _cached_data[cache_key]


def build_space_config(scalar_index_type: str, space_name: str):
    properties = {}
    properties["fields"] = [
        {
            "name": "field_cardinality_1",
            "type": "integer",
            "index": {"name": "field_cardinality_1", "type": scalar_index_type},
        },
        {
            "name": "field_cardinality_2",
            "type": "integer",
            "index": {"name": "field_cardinality_2", "type": scalar_index_type},
        },
        {
            "name": "field_cardinality_10",
            "type": "integer",
            "index": {"name": "field_cardinality_10", "type": scalar_index_type},
        },
        {
            "name": "field_cardinality_50",
            "type": "integer",
            "index": {"name": "field_cardinality_50", "type": scalar_index_type},
        },
        {
            "name": "field_vector",
            "type": "vector",
            "dimension": 128,
            "store_type": "MemoryOnly",
            "index": {
                "name": "gamma",
                "type": "FLAT",
                "params": {
                    "ncentroids": 4096,
                    "nlinks": 32,
                    "metric_type": "L2",
                    "efConstruction": 100,
                },
            },
        },
    ]
    space_config = {
        "name": space_name,
        "partition_num": 1,
        "replica_num": 1,
        "fields": properties["fields"],
    }
    return space_config


def _create_space(router_url, scalar_index_type: str, space_name: str):
    space_config = build_space_config(scalar_index_type, space_name)
    response = create_db(router_url, db_name)
    logger.info(response.json())
    assert response.json()["code"] == 0
    response = create_space(router_url, db_name, space_config)
    logger.info(response.json())
    assert response.json()["code"] == 0


def _add_benchmark_data(total_batches: int, batch_size: int,
                         vectors: np.ndarray, cardinality_map: dict, space_name: str):
    pool = ThreadPool()
    total_data = []
    for i in range(total_batches):
        start_idx = i * batch_size
        end_idx = start_idx + batch_size
        batch_vectors = vectors[start_idx:end_idx]
        batch_cardinalities = {
            field_name: values[start_idx:end_idx]
            for field_name, values in cardinality_map.items()
        }
        total_data.append((i, batch_size, batch_vectors, batch_cardinalities, db_name, space_name))

    results = pool.map(_process_add_scalar_data, total_data)
    pool.close()
    pool.join()
    failed = sum(1 for r in results if r != 0)
    if failed > 0:
        logger.error(f"{failed} batches failed out of {total_batches}")


def _process_add_scalar_data(items):
    url = router_url + "/document/upsert?timeout=2000000"
    batch_idx, batch_size, batch_vectors, batch_cardinalities, db_name, space_name = items
    data = {
        "db_name": db_name,
        "space_name": space_name,
        "documents": [
            {
                "_id": str(batch_idx * batch_size + j),
                **{field_name: int(values[j]) for field_name, values in batch_cardinalities.items()},
                "field_vector": batch_vectors[j].tolist(),
            }
            for j in range(batch_size)
        ],
    }
    rs = requests.post(url, auth=(username, password), json=data)
    if rs.json()["code"] != 0:
        logger.error(f"Batch {batch_idx} failed: {rs.json()}")
    return rs.json().get("code", -1)


def _query_one_condition(scalar_index_type: str, space_name: str):
    """One-condition range query across all cardinality fields."""
    url = router_url + "/document/query"
    results = []
    limit = 100
    for cardinality, field_name in CARDINALITY_CONFIGS.items():
        if cardinality > 50:
            continue
        low = cardinality // 4
        high = (cardinality if cardinality > 1 else cardinality // 2) + 1
        query_dict = {
            "db_name": db_name,
            "space_name": space_name,
            "filters": {
                "operator": "AND",
                "conditions": [
                    {"field": field_name, "operator": ">=", "value": low},
                    {"field": field_name, "operator": "<", "value": high},
                ],
            },
            "limit": limit,
        }
        json_str = json.dumps(query_dict)
        t0 = time.time()
        response = requests.post(url, auth=(username, password), data=json_str)
        t1 = time.time()
        elapsed_ms = (t1 - t0) * 1000.0

        assert response.status_code == 200
        assert response.json()["code"] == 0
        total = response.json()["data"]["total"]
        docs = response.json()["data"]["documents"]
        assert len(docs) == limit
        for doc in docs:
            assert low <= doc[field_name] < high

        results.append({
            "scalar_index_type": scalar_index_type,
            "query_type": "one_condition",
            "field_name": field_name,
            "cardinality": cardinality,
            "condition": f"[{low}, {high})",
            "elapsed_ms": elapsed_ms,
        })
        logger.info(
            f"[{scalar_index_type}] one-condition query {field_name}(cardinality={cardinality}) "
            f"[{low}, {high}): {total} docs, {elapsed_ms:.2f}ms"
        )

    return results


# Multi-condition query field combinations: list of (field_name, low, high)
MULTI_CONDITION_COMBOS = [
    # (field_cardinality_1, field_cardinality_2)
    ("field_cardinality_1", "field_cardinality_2", 0, 1, 0, 2),
    # (field_cardinality_1, field_cardinality_10)
    ("field_cardinality_1", "field_cardinality_10", 0, 1, 2, 6),
    # (field_cardinality_1, field_cardinality_50)
    ("field_cardinality_1", "field_cardinality_50", 0, 1, 20, 40),
    # (field_cardinality_2, field_cardinality_10)
    ("field_cardinality_2", "field_cardinality_10", 0, 2, 2, 6),
    # (field_cardinality_2, field_cardinality_50)
    ("field_cardinality_2", "field_cardinality_50", 0, 2, 20, 40),
    # (field_cardinality_10, field_cardinality_50)
    ("field_cardinality_10", "field_cardinality_50", 2, 6, 20, 40),
]


# Composite index field ordering scenarios (for comparison)
# The composite index includes cardinality_50 (high cardinality, 50 unique values)
# and cardinality_2 (low cardinality, 2 unique values).
#
# Performance insight:
# - high_first: cardinality_50 first -> first field query scans ~2% of data (good selectivity)
# - low_first: cardinality_2 first -> first field query scans ~50% of data (poor selectivity)
COMPOSITE_FIELD_ORDER_SCENARIOS = {
    "high_first": {
        "fields": ["field_cardinality_50", "field_cardinality_2"],
        "description": "High cardinality (50) first: better prefix selectivity, faster query",
    },
    "low_first": {
        "fields": ["field_cardinality_2", "field_cardinality_50"],
        "description": "Low cardinality (2) first: poor prefix selectivity, slower query",
    },
}


# Composite index query configurations
# Each config: (description, [(field_name, value), ...])
#
# field_name: which field to query (matches field_order in the scenario)
# value: exact value for Equal query
#
# These queries are applied to BOTH "high_first" and "low_first" scenarios,
# where the composite fields are ["field_cardinality_50", "field_cardinality_2"].
# The query only applies conditions to fields that exist in the composite index.
COMPOSITE_QUERY_CONFIGS = [
    # Q1: dual-field query on composite index
    # For high_first  (c50 first, c2 second): prefix selectivity ~1/50 -> fast
    # For low_first   (c2 first, c50 second): prefix selectivity ~1/2  -> slow
    (
        "dual_field: c50=5 AND c2=1",
        [
            ("field_cardinality_50", 5),
            ("field_cardinality_2", 1),
        ],
    ),
    # Q2: single-field query on FIRST composite field only
    # For high_first: selectivity ~1/50 = 2%  -> 1st field = high cardinality, good selectivity
    # For low_first:  selectivity ~1/2  = 50% -> 1st field = low cardinality, no prefix benefit
    (
        "first_field_only: c50=10",
        [
            ("field_cardinality_50", 10),
        ],
    ),
    # Q3: first field has specific value -> moderate selectivity regardless of cardinality
    (
        "first_field_single: c50=0",
        [
            ("field_cardinality_50", 0),
        ],
    ),
    # Q4: dual-field with selective first field
    # high_first: c50 selectivity ~1/50 + c2 filtering -> very selective
    # low_first:  c2 selectivity ~50% (no prefix help), then c50 -> still selective
    (
        "dual_selective: c50=25 AND c2=0",
        [
            ("field_cardinality_50", 25),
            ("field_cardinality_2", 0),
        ],
    ),
]


def _query_composite(space_name: str, scenario_name: str, field_order: list):
    """Query composite index with different field combinations.

    For each query config, we build conditions targeting the composite index fields.
    Conditions are applied only to fields that are part of the composite index.

    Query configs: (description, [(field_name, value), ...])
    - field_name: which field to query
    - value: exact value for Equal query
    """
    url = router_url + "/document/query"
    results = []
    limit = 100

    composite_set = set(field_order)

    for description, field_conditions in COMPOSITE_QUERY_CONFIGS:
        # Build conditions: only include fields that are in the composite index
        conditions = []
        for field_name, value in field_conditions:
            if field_name in composite_set:
                conditions.append({"field": field_name, "operator": "=", "value": value})

        if not conditions:
            continue

        # Check if all queried fields are prefix of the composite index
        # Composite index supports queries where conditions match prefix order
        # e.g., [A, B]: query(A) works, query(A,B) works, query(B) does NOT work
        queried_in_order = []
        for field_name, _ in field_conditions:
            if field_name in composite_set:
                # Find position in composite field order
                for i, f in enumerate(field_order):
                    if f == field_name:
                        queried_in_order.append((i, field_name))
                        break

        # Check if conditions match the prefix of field_order
        # Sort by index to verify prefix matching
        queried_in_order_sorted = sorted(queried_in_order, key=lambda x: x[0])
        expected_prefix = list(range(len(queried_in_order_sorted)))
        actual_indices = [idx for idx, _ in queried_in_order_sorted]
        is_prefix_query = (expected_prefix == actual_indices)

        query_dict = {
            "db_name": db_name,
            "space_name": space_name,
            "filters": {
                "operator": "AND",
                "conditions": conditions,
            },
            "limit": limit,
        }

        json_str = json.dumps(query_dict)
        logger.info(f"Query: {json_str}")
        t0 = time.time()
        response = requests.post(url, auth=(username, password), data=json_str)
        t1 = time.time()
        elapsed_ms = (t1 - t0) * 1000.0

        assert response.status_code == 200, f"Request failed: {response.status_code}" + f" {response.text}"
        assert response.json()["code"] == 0, f"Query failed: {response.json()}"

        docs = response.json()["data"]["documents"]
        total = response.json()["data"]["total"]
        # logger.info(response.json()["data"])

        # If query is not a prefix of composite index, it may not use composite index
        # and may return fewer results than limit
        if is_prefix_query:
            assert len(docs) == limit, f"Prefix query should return {limit} docs, got {len(docs)}"
        else:
            # Non-prefix query might return fewer results, just log it
            logger.info(
                f"  Note: query is not prefix of composite index, "
                f"total={total}, returned={len(docs)}"
            )

        results.append({
            "scenario": scenario_name,
            "field_order": field_order,
            "description": description,
            "conditions": conditions,
            "total_docs": limit,
            "returned_docs": len(docs),
            "total": total,
            "is_prefix_query": is_prefix_query,
            "elapsed_ms": elapsed_ms,
        })

        queried_fields = [fn for fn, _ in field_conditions if fn in composite_set]
        prefix_note = " [PREFIX]" if is_prefix_query else " [NON-PREFIX]"
        logger.info(
            f"[Composite {scenario_name}] {description}{prefix_note}: "
            f"fields={queried_fields}, "
            f"total={total}, returned={len(docs)}, {elapsed_ms:.2f}ms"
        )

    return results


def build_composite_space_config(field_order: list, space_name: str):
    """Build space config with composite index on specified field order.

    Args:
        field_order: List of field names to include in composite index (in order).
                     The composite index is defined in the "indexes" array, so fields
                     need not be consecutive in the fields list.
        space_name: Name of the space
    """
    # Build fields list: scalar fields with INVERTED index, no COMPOSITE in fields
    scalar_fields = [
        "field_cardinality_1",
        "field_cardinality_2",
        "field_cardinality_10",
        "field_cardinality_50",
    ]

    fields_def = []
    for fname in scalar_fields:
        fields_def.append({
            "name": fname,
            "type": "integer",
            "index": {"name": fname, "type": "INVERTED"},
        })

    # Add vector field at the end
    fields_def.append({
        "name": "field_vector",
        "type": "vector",
        "dimension": 128,
        "store_type": "MemoryOnly",
        "index": {
            "name": "gamma",
            "type": "FLAT",
            "params": {
                "ncentroids": 4096,
                "nlinks": 32,
                "metric_type": "L2",
                "efConstruction": 100,
            },
        },
    })

    # Composite index is defined in the indexes array, fields need not be consecutive
    space_config = {
        "name": space_name,
        "partition_num": 1,
        "replica_num": 1,
        "fields": fields_def,
        "indexes": [
            {
                "name": "idx_composite",
                "type": "COMPOSITE",
                "field_names": list(field_order),
            },
        ],
    }
    return space_config


def _query_multi_conditions(scalar_index_type: str, space_name: str):
    """Multi-condition queries across all two-field combinations."""
    url = router_url + "/document/query"
    results = []
    limit = 100

    for f1, f2, lo1, hi1, lo2, hi2 in MULTI_CONDITION_COMBOS:
        conditions = [
            {"field": f1, "operator": ">=", "value": lo1},
            {"field": f1, "operator": "<", "value": hi1},
            {"field": f2, "operator": ">=", "value": lo2},
            {"field": f2, "operator": "<", "value": hi2},
        ]
        query_dict = {
            "db_name": db_name,
            "space_name": space_name,
            "filters": {"operator": "AND", "conditions": conditions},
            "limit": limit,
        }
        json_str = json.dumps(query_dict)
        t0 = time.time()
        response = requests.post(url, auth=(username, password), data=json_str)
        t1 = time.time()
        elapsed_ms = (t1 - t0) * 1000.0

        assert response.status_code == 200
        assert response.json()["code"] == 0
        docs = response.json()["data"]["documents"]
        assert len(docs) == limit
        for doc in docs:
            assert lo1 <= doc[f1] < hi1
            assert lo2 <= doc[f2] < hi2

        results.append({
            "scalar_index_type": scalar_index_type,
            "query_type": "multi_condition",
            "fields": f"{f1} & {f2}",
            "condition": f"{f1} in [{lo1},{hi1}) AND {f2} in [{lo2},{hi2})",
            "elapsed_ms": elapsed_ms,
        })
        logger.info(
            f"[{scalar_index_type}] multi-condition {f1}&{f2}: "
            f"{f1} in [{lo1},{hi1}), {f2} in [{lo2},{hi2}), "
            f"{elapsed_ms:.2f}ms"
        )

    return results


def benchmark_scalar_index(scalar_index_type: str, vectors: np.ndarray,
                            cardinality_map: dict, total_docs: int = 1_000_000,
                            batch_size: int = 200, space_name: str = "scalar_index_benchmark"):
    """
    Full benchmark for one scalar_index_type:
      1. create space
      2. insert all data
      3. wait index finish
      4. one-condition query
      5. multi-condition query
      6. destroy space
    Returns (insert_elapsed_s, insert_throughput, one_cond_results, multi_cond_results).
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"Scalar Index Type: {scalar_index_type}")
    logger.info(f"Total docs: {total_docs}, Batch size: {batch_size}, Dim: 128")
    logger.info(f"{'='*60}")

    # 1. create space
    _create_space(router_url, scalar_index_type, space_name)

    # 2. insert
    t0 = time.time()
    total_batches = total_docs // batch_size
    remainder = total_docs - total_batches * batch_size
    _add_benchmark_data(total_batches, batch_size, vectors, cardinality_map, space_name)
    if remainder > 0:
        rem_vectors = vectors[total_batches * batch_size:]
        rem_cardinalities = {
            field_name: values[total_batches * batch_size:]
            for field_name, values in cardinality_map.items()
        }
        _add_benchmark_data(1, remainder, rem_vectors, rem_cardinalities, space_name)
    t1 = time.time()
    insert_elapsed = t1 - t0
    insert_throughput = total_docs / insert_elapsed
    logger.info(f"[{scalar_index_type}] Insert: {insert_elapsed:.2f}s, {insert_throughput:.2f} docs/s")

    # 3. wait index
    waiting_index_finish(total_docs, space_name=space_name)

    # 4. one-condition query
    one_cond_results = _query_one_condition(scalar_index_type, space_name)

    # 5. multi-condition query
    multi_cond_results = _query_multi_conditions(scalar_index_type, space_name)

    # 6. destroy
    destroy(router_url, db_name, space_name)

    return insert_elapsed, insert_throughput, one_cond_results, multi_cond_results


def _create_composite_space(router_url, space_config: dict):
    """Create space for composite index benchmark."""
    response = create_db(router_url, db_name)
    logger.info(response.json())
    assert response.json()["code"] == 0
    response = create_space(router_url, db_name, space_config)
    logger.info(response.json())
    assert response.json()["code"] == 0


def benchmark_composite_index(vectors: np.ndarray, cardinality_map: dict,
                                 total_docs: int = 1_000_000, batch_size: int = 200):
    """
    Benchmark composite index with different field ordering scenarios.

    This function tests the performance impact of field ordering in composite indexes:
    - Scenario 1: low cardinality fields first (optimal prefix selectivity)
    - Scenario 2: low cardinality fields last (poor prefix selectivity)
    - Scenario 3: mixed ordering (medium selectivity)

    Returns a dict mapping scenario_name -> list of query results.
    """
    logger.info(f"\n{'='*60}")
    logger.info("Composite Index Benchmark: Field Order Comparison")
    logger.info(f"Total docs: {total_docs}, Batch size: {batch_size}, Dim: 128")
    logger.info(f"{'='*60}")

    all_results = {}

    for scenario_name, scenario_config in COMPOSITE_FIELD_ORDER_SCENARIOS.items():
        field_order = scenario_config["fields"]
        description = scenario_config["description"]

        logger.info(f"\n--- Scenario: {scenario_name} ---")
        logger.info(f"Field order: {field_order}")
        logger.info(f"Description: {description}")

        space_name = f"composite_benchmark_{scenario_name}"

        # 1. Create space
        space_config = build_composite_space_config(field_order, space_name)
        _create_composite_space(router_url, space_config)

        # 2. Insert data
        t0 = time.time()
        total_batches = total_docs // batch_size
        remainder = total_docs - total_batches * batch_size
        _add_benchmark_data(total_batches, batch_size, vectors, cardinality_map, space_name)
        if remainder > 0:
            rem_vectors = vectors[total_batches * batch_size:]
            rem_cardinalities = {
                field_name: values[total_batches * batch_size:]
                for field_name, values in cardinality_map.items()
            }
            _add_benchmark_data(1, remainder, rem_vectors, rem_cardinalities, space_name)
        t1 = time.time()
        insert_elapsed = t1 - t0
        insert_throughput = total_docs / insert_elapsed
        logger.info(f"[{scenario_name}] Insert: {insert_elapsed:.2f}s, {insert_throughput:.2f} docs/s")

        # 3. Wait index finish
        waiting_index_finish(total_docs, space_name=space_name)

        # 4. Query
        query_results = _query_composite(space_name, scenario_name, field_order)
        all_results[scenario_name] = {
            "field_order": field_order,
            "description": description,
            "insert_elapsed": insert_elapsed,
            "insert_throughput": insert_throughput,
            "query_results": query_results,
        }

        # 5. Destroy space
        destroy(router_url, db_name, space_name)

    # Summary comparison
    _print_composite_benchmark_summary(all_results)

    return all_results


def _print_composite_benchmark_summary(all_results: dict):
    """Print a summary comparison of all composite index scenarios."""
    logger.info(f"\n{'='*60}")
    logger.info("COMPOSITE INDEX BENCHMARK SUMMARY")
    logger.info(f"{'='*60}")

    # Collect all query types
    query_types = set()
    for scenario_name, result in all_results.items():
        for qr in result["query_results"]:
            query_types.add(qr["description"])

    # Print comparison table header
    logger.info(f"\n{'Scenario':<20} {'Field Order':<50} {'Insert Time':<12} {'Throughput':<15}")
    logger.info("-" * 100)
    for scenario_name, result in all_results.items():
        logger.info(
            f"{scenario_name:<20} {str(result['field_order']):<50} "
            f"{result['insert_elapsed']:.2f}s{'':<6} {result['insert_throughput']:.0f} docs/s"
        )

    # Print query performance comparison
    logger.info(f"\n{'Query Type':<55} {'Scenario':<20} {'Total Docs':<12} {'Time (ms)':<10}")
    logger.info("-" * 100)

    for scenario_name, result in all_results.items():
        for qr in result["query_results"]:
            logger.info(
                f"{qr['description']:<55} {scenario_name:<20} "
                f"{qr['total_docs']:<12} {qr['elapsed_ms']:.2f}"
            )
        logger.info("")

    # Highlight performance differences: high_first vs low_first
    logger.info("\n" + "=" * 60)
    logger.info("PERFORMANCE COMPARISON: high_first vs low_first")
    logger.info("=" * 60)
    logger.info("high_first: cardinality_50 (high cardinality) FIRST -> better selectivity")
    logger.info("low_first: cardinality_2 (low cardinality) FIRST -> poor selectivity")

    if "high_first" in all_results and "low_first" in all_results:
        high_first_queries = {qr["description"]: qr for qr in all_results["high_first"]["query_results"]}
        low_first_queries = {qr["description"]: qr for qr in all_results["low_first"]["query_results"]}

        for desc in high_first_queries:
            if desc in low_first_queries:
                hf_time = high_first_queries[desc]["elapsed_ms"]
                lf_time = low_first_queries[desc]["elapsed_ms"]
                hf_docs = high_first_queries[desc]["total_docs"]
                lf_docs = low_first_queries[desc]["total_docs"]
                diff_pct = ((lf_time - hf_time) / hf_time * 100) if hf_time > 0 else 0
                faster = "high_first" if hf_time < lf_time else "low_first"
                logger.info(
                    f"{desc}:\n"
                    f"  high_first: {hf_time:.2f}ms ({hf_docs} docs)\n"
                    f"  low_first: {lf_time:.2f}ms ({lf_docs} docs)\n"
                    f"  difference: {diff_pct:+.1f}%, faster: {faster}"
                )


@pytest.fixture(scope="module")
def scalar_benchmark_data():
    """Module-scoped fixture: generate vectors and cardinality data once, reuse for all index types."""
    total_docs = 1_000_000
    embedding_dim = 128
    vectors, cardinality_map = _generate_cached_data(total_docs, embedding_dim)
    return {"total_docs": total_docs, "embedding_dim": embedding_dim,
            "vectors": vectors, "cardinality_map": cardinality_map}


@pytest.mark.parametrize("scalar_index_type", SCALAR_INDEX_TYPES)
def test_scalar_index_benchmark_1m(scalar_index_type: str, scalar_benchmark_data: dict):
    batch_size = 200
    vectors = scalar_benchmark_data["vectors"]
    cardinality_map = scalar_benchmark_data["cardinality_map"]
    total_docs = scalar_benchmark_data["total_docs"]
    space_name = f"scalar_index_benchmark_{scalar_index_type}"
    insert_elapsed, insert_throughput, one_cond_results, multi_cond_results = \
        benchmark_scalar_index(scalar_index_type, vectors, cardinality_map,
                               total_docs, batch_size, space_name)

    total_query_elapsed = sum(r["elapsed_ms"]
                              for r in one_cond_results + multi_cond_results)

    logger.info(f"[{scalar_index_type}] Insert: {insert_elapsed:.2f}s, {insert_throughput:.2f} docs/s")
    logger.info(
        f"[{scalar_index_type}] Query: "
        f"{len(one_cond_results)} one-condition + {len(multi_cond_results)} multi-condition, "
        f"total time: {total_query_elapsed:.2f}ms"
    )

    assert insert_elapsed > 0
    assert insert_throughput > 0
    assert len(one_cond_results) > 0
    assert len(multi_cond_results) > 0


def test_composite_index_field_order_benchmark(scalar_benchmark_data: dict):
    """
    Benchmark composite index with different field ordering scenarios.

    This test compares two scenarios:
    1. high_first: High cardinality (50) field FIRST -> better prefix selectivity
       Fields: [cardinality_50, cardinality_2]
       Query: cardinality_50=5 AND cardinality_2=1 -> ~2% of data matches
    2. low_first: Low cardinality (2) field FIRST -> poor prefix selectivity
       Fields: [cardinality_2, cardinality_50]
       Query: cardinality_2=1 AND cardinality_50=5 -> ~50% of data matches

    Key insight: When the high-cardinality field comes first in the composite index,
    the first field filter is more selective (returns fewer results), making the
    composite index query faster due to reduced RocksDB scan range.
    """
    batch_size = 200
    vectors = scalar_benchmark_data["vectors"]
    cardinality_map = scalar_benchmark_data["cardinality_map"]
    total_docs = scalar_benchmark_data["total_docs"]

    all_results = benchmark_composite_index(
        vectors, cardinality_map, total_docs, batch_size
    )

    # Verify all scenarios completed
    assert len(all_results) == len(COMPOSITE_FIELD_ORDER_SCENARIOS)
    for scenario_name in COMPOSITE_FIELD_ORDER_SCENARIOS:
        assert scenario_name in all_results
        assert len(all_results[scenario_name]["query_results"]) > 0

    # Basic assertions
    assert all(r["insert_elapsed"] > 0 for r in all_results.values())
    assert all(r["insert_throughput"] > 0 for r in all_results.values())
    assert all(len(r["query_results"]) > 0 for r in all_results.values())

    logger.info("\nComposite index field order benchmark completed successfully.")
    logger.info("Check the summary above for performance comparison between scenarios.")
