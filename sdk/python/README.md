# Vearch Python SDK

This README provides examples on how to use the Vearch Python SDK for interacting with Vearch, a scalable distributed system for embedding-based retrieval.

## Prerequisites

Before you begin, ensure you have the following:

- Python 3.7 and above.
- Access to a running Vearch server.

## Compatibility

The following table shows Vearch versions and recommended PyVearch versions:

| Vearch version | Recommended PyVearch version |
|:--------------:|:----------------------------:|
|      3.5.X     |             3.5.1            |

## Installation

Install Vearch via pip or pip3:

```sh
pip3 install pyvearch
```

## Usage Examples

### Setup Client

```python
from vearch.core.vearch import Vearch
from vearch.config import Config

config = Config(host="your router path", token="secret")
vc = Vearch(config)
```

### Creating a Database and Space

```python
from vearch.schema.field import Field
from vearch.schema.space import SpaceSchema
from vearch.utils import DataType, MetricType, VectorInfo
from vearch.schema.index import FlatIndex, ScalarIndex

ret = vc.create_database("database_test")
print("create db: ", ret.dict_str())

book_name = Field("book_name", DataType.STRING, desc="the name of book", index=ScalarIndex("book_name_idx"))
book_num=Field("book_num", DataType.INTEGER, desc="the num of book",index=ScalarIndex("book_num_idx"))
book_vector = Field("book_character", DataType.VECTOR, FlatIndex("book_vec_idx", MetricType.Inner_product), dimension=512)
space_schema = SpaceSchema("book_info", fields=[book_name,book_num, book_vector])

ret = vc.create_space("database_test", space_schema)
print("create space: ", ret.data)
```

### Inserting Documents

```python
import random
data = []
book_item = {"book_name": "Write", "book_num": 7, "book_character":[random.uniform(0, 1) for _ in range(512)]}
data.append(book_item)
ret = vc.upsert("database_test", "book_info", data)
document_ids = ret.get_document_ids()
```

### Searching Documents

```python
import random
feature = [random.uniform(0, 1) for _ in range(512)]
vi = VectorInfo("book_character", feature)
ret = vc.search("database_test", "book_info",vector_infos=[vi, ],limit=7)
print("search document", ret.documents)
```

### Querying Documents

```python
ret = vc.query("database_test", "book_info", document_ids)
print(ret.documents)
```

### Deleting Documents

```python
ret = vc.delete("database_test", "book_info", document_ids)
print(ret.document_ids)
```

### More

[Example](../../examples/python/example.py)

## Running Tests

To run the provided tests, execute the following command in your terminal:

```sh
pytest test/  # Run this in the directory where your test files are located
```

Make sure your Vearch server is running and accessible at the specified host address before running the tests.
