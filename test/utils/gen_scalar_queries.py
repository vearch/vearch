#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""
Generate random two-field combination queries and save to JSON lines file.
Used for batch query benchmark across scalar index types.
"""

import json
import random
from pathlib import Path


CARDINALITY_CONFIGS = {
    1: "field_cardinality_1",
    2: "field_cardinality_2",
    10: "field_cardinality_10",
    50: "field_cardinality_50",
}

# All pairs of field names (ordered, no duplicates)
FIELD_PAIRS = [
    (f1, f2)
    for i, f1 in enumerate(CARDINALITY_CONFIGS.values())
    for f2 in list(CARDINALITY_CONFIGS.values())[i + 1:]
]


FIELD_NAME_TO_CARDINALITY = {v: k for k, v in CARDINALITY_CONFIGS.items()}


def random_range(field_name: str, cardinality: int):
    """Generate a random range condition for a given field."""
    if cardinality == 1:
        lo = 0
        hi = 1
    elif cardinality == 2:
        lo = random.randint(0, 0)
        hi = random.randint(lo + 1, 2)
    else:
        lo = random.randint(0, max(0, cardinality - 3))
        span = random.randint(1, max(1, cardinality // 3))
        hi = min(lo + span, cardinality)
    return {"field": field_name, "operator": ">=", "value": lo}, \
           {"field": field_name, "operator": "<", "value": hi}


def random_eq(field_name: str, cardinality: int):
    """Generate a random equality condition for a given field."""
    value = random.randint(0, max(0, cardinality - 1))
    return {"field": field_name, "operator": "=", "value": value}


def generate_queries(num_queries: int, db_name: str = "default",
                     space_name: str = "test_space") -> list[dict]:
    """
    Generate `num_queries` random two-field combination queries.
    Each query randomly picks two different fields and two random conditions.
    """
    queries = []
    for _ in range(num_queries):
        f1_name, f2_name = random.choice(FIELD_PAIRS)
        c1 = FIELD_NAME_TO_CARDINALITY[f1_name]
        c2 = FIELD_NAME_TO_CARDINALITY[f2_name]

        # Randomly choose condition type: range or equality
        if random.random() < 0.5:
            c1a, c1b = random_range(f1_name, c1)
            c2a, c2b = random_range(f2_name, c2)
        else:
            c1a = random_eq(f1_name, c1)
            c2a = random_eq(f2_name, c2)
            c1b, c2b = None, None

        conditions = [c1a, c2a]
        if c1b:
            conditions.append(c1b)
        if c2b:
            conditions.append(c2b)

        query = {
            "db_name": db_name,
            "space_name": space_name,
            "filters": {"operator": "AND", "conditions": conditions},
            "limit": 100,
        }
        queries.append(query)

    return queries


def save_queries_jsonl(queries: list[dict], output_path: str | Path):
    """Save queries to a JSON lines file, one JSON object per line."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        for q in queries:
            f.write(json.dumps(q, ensure_ascii=False) + "\n")
    return output_path


def load_queries_jsonl(input_path: str | Path) -> list[dict]:
    """Load queries from a JSON lines file."""
    queries = []
    with open(input_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                queries.append(json.loads(line))
    return queries


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Generate random two-field query JSON lines for scalar index benchmark"
    )
    parser.add_argument("-n", "--num", type=int, default=10_000,
                        help="Number of queries to generate (default: 10000)")
    parser.add_argument("-o", "--output", type=str,
                        default="test/scalar_queries.jsonl",
                        help="Output JSON lines file path (default: test/scalar_queries.jsonl)")
    parser.add_argument("--db", type=str, default="default",
                        help="db_name in queries (default: default)")
    parser.add_argument("--space", type=str, default="test_space",
                        help="space_name in queries (default: test_space)")
    args = parser.parse_args()

    print(f"Generating {args.num} random two-field queries ...")
    queries = generate_queries(args.num, db_name=args.db, space_name=args.space)

    output_path = save_queries_jsonl(queries, args.output)

    # Show sample
    print(f"Saved {len(queries)} queries to {output_path}")
    print("\nSample queries:")
    for i, q in enumerate(queries[:3]):
        print(f"  [{i + 1}] {json.dumps(q, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
