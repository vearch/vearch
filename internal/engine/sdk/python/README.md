# Vearch Engine Python SDK

vearch Engine python sdk and python wheel packages.

## Overview

This repository shows vearch engine python sdk and provides scripts to create wheel
packages for the vearch library.

Files in directory of python shows how the python sdk encapsulate vearch.
setup.py is written for creating wheel packages for vearch.

Of course, pip install vearch is the easiest way to use this python sdk. And
this repository helps to build your custom python sdk.

## Usage

### Create engine

```python
from vearch import Config, Engine
config = Config("datas", "logs")
engine = Engine(config)
```

### Create table

```python
from vearch import Table
from vearch import FieldInfo, VectorInfo
from vearch import dataType
from vearch.schema.index import HNSWIndex, MetricType
field_infos = [FieldInfo("field_long", dataType.LONG), FieldInfo("field_string", dataType.STRING, True), FieldInfo("field_float", dataType.FLOAT, True), FieldInfo("field_double", dataType.DOUBLE, True), FieldInfo("field_int", dataType.INT, True),FieldInfo("field_string_array", dataType.STRINGARRAY, True), FieldInfo("field_date", dataType.DATE, True)]
vector_infos = [VectorInfo(name="field_vector", dimension=64)]
index = HNSWIndex("vec_index", nlinks=32, efConstruction=120,metric_type=MetricType.L2)
table=Table(name="test_table",field_infos=field_infos, vector_infos=vector_infos, index=index)
response = engine.create_table(table)
print(response.to_dict())
```

### add or update documents

```python
from vearch import Document, Field, dataType
import numpy as np
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
doc1.to_fields(table)

fields = [
    Field(name="field_long", value=1, data_type=dataType.LONG),
    Field(name="field_string", value="1", data_type=dataType.STRING),
    Field(name="field_float", value=1.0, data_type=dataType.FLOAT),
    Field(name="field_double", value=1.0, data_type=dataType.DOUBLE),
    Field(name="field_int", value=1, data_type=dataType.INT),
    Field(name="field_string_array", value=["1", "1"], data_type=dataType.STRINGARRAY),
    Field(name="field_date", value="2020-02-02", data_type=dataType.DATE),
    Field(name="field_vector", value=np.random.rand(64).astype('float32'), data_type=dataType.VECTOR)
]
doc2 = Document(fields=fields)
upsert_response = engine.upsert([doc1, doc2])
print(upsert_response.to_dict())

# update
datas = {
    "_id": upsert_response.document_ids[0][0],
    "field_long": 2,
    "field_string": "2",
    "field_float": 2.0,
    "field_double": 2.0,
    "field_int": 2,
    "field_string_array": ["2"],
    "field_date": "2020-02-02"
}
doc3 = Document(datas=datas)
doc3.to_fields(table)
response = engine.upsert([doc3])
print(response.to_dict())
```

### get document

```python
# get by document id
response = engine.get(upsert_response.document_ids[0][0])
print(response.to_dict())

# get ducument by index
response = engine.get(0)
print(response.to_dict())
```

### query document

```python
from vearch import QueryRequest, TermFilter, RangeFilter, FilterBooleanOperator, FilterRelationOperator

# query by document_ids
query_request = QueryRequest(
    document_ids = [upsert_response.document_ids[0][0]]
)
response = engine.query(query_request)
print(response.to_dict())

# query by filter
term_filter = TermFilter(field_name="field_string", value="1", filter_operator=FilterRelationOperator.IN)
range_filter = RangeFilter(
    field="field_float",
    lower_value=0.5,
    upper_value=1.5,
    include_lower=True,
    include_upper=False
)
query_request = QueryRequest(
    term_filters = [term_filter],
    range_filters = [range_filter]
)
response = engine.query(query_request)
print(response.to_dict())

```

### search document

```python
from vearch import VectorQuery, SearchRequest

# query by filter
vector_query = VectorQuery(name="field_vector", value = np.random.rand(1, 64).astype('float32'))
search_request = SearchRequest(
    limit=10,
    term_filters = [term_filter],
    range_filters = [range_filter],
    vec_fields = [vector_query]
)
response = engine.search(search_request)
print(response.to_dict())

```

### delete document

```python
response = engine.delete(upsert_response.document_ids[0][0])
print(response.to_dict())
```

### engine status

```python
status = engine.status()
```

### close engine

```python
status = engine.close()
```

### create backup for restore

Before creating a backup, ensure that the backup directory is mounted to an S3-compatible storage.

```python
from vearch import Config, BackupConfig, Engine, Table, FieldInfo, VectorInfo, Document
from vearch.schema.index import HNSWIndex, MetricType
from vearch import dataType

# Define backup configuration
backup_config = BackupConfig(
    cluster_name="test-token98",
    cluster_id=10770,
    db_name="db",
    db_id=1,
    space_name="test_s3_space",
    partition_num=2,
    replica_num=3,
    backup_id=1
)
s3_path = "/mnt/cfs/"
backup_path = f"{s3_path}{backup_config.cluster_name}_{backup_config.cluster_id}/backup/{backup_config.db_name}/{backup_config.space_name}/{backup_config.backup_id}/"

# Create multiple engine instances for different partitions
engines = []
for partition_id in range(backup_config.partition_num):
    config = Config(
        path=backup_path + str(partition_id),
        log_dir=str(partition_id) + "_logs",
        backup_config=backup_config
    )
    engine = Engine(config)
    engines.append(engine)

# Define table schema
field_infos = [
    FieldInfo("field_long", dataType.LONG),
    FieldInfo("field_string", dataType.STRING, True),
    FieldInfo("field_float", dataType.FLOAT, True),
    FieldInfo("field_double", dataType.DOUBLE, True),
    FieldInfo("field_int", dataType.INT, True),
    FieldInfo("field_string_array", dataType.STRINGARRAY, True),
    FieldInfo("field_date", dataType.DATE, True)
]
vector_infos = [VectorInfo(name="field_vector", dimension=64)]
index = HNSWIndex("vec_index", nlinks=32, efConstruction=120, metric_type=MetricType.L2)
# set table name as partition id
table = Table(name="0", field_infos=field_infos, vector_infos=vector_infos, index=index)

# Create table in each engine instance
for i, engine in enumerate(engines):
    table.name = str(i)
    response = engine.create_table(table)
    print(response.to_dict())

# Add or update documents in each partition
import numpy as np
for partition_id, engine in enumerate(engines):
    features = np.random.rand(64).astype('float32')
    datas = {
        "field_long": partition_id,
        "field_string": str(partition_id),
        "field_float": float(partition_id),
        "field_double": float(partition_id),
        "field_int": partition_id,
        "field_string_array": [str(partition_id)],
        "field_date": "2020-02-02",
        "field_vector": features
    }
    doc = Document(datas=datas)
    doc.to_fields(table)
    response = engine.upsert([doc])
    print(response.to_dict())

# Close engines after upsert
for engine in engines:
    engine.close()
```

This setup allows you to create backups for each partition and load pre-built SST files for offline construction.
This way, you can quickly import data using the backup interface.

## Building source package

if there is a custom built vearch library in the system, build source package
for the best performance.

### Prerequisite

You can build it with docker image: docker.io/dockcross/manylinux2014-x64:latest

auditwheel tool should be installed firstly. You can install it by pip.

The package can be built when gamma is already built and installed.
See the official [gamma installation
instruction](https://github.com/vearch/vearch/blob/master/internal/engine/README.md) for more
on how to build and install gamma. In particular, compiling wheel packages
requires additional compilation options in compiling gamma.

```bash
git clone https://github.com/vearch/vearch.git
cd vearch/internal/engine
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DPERFORMANCE_TESTING=ON ..
make
sh build-wheels.sh
sh install-vearch.sh
```

Then the whl file will be generated into the wheelhouse directory.

For building wheel packages, swig 4.0.2 or later needs to be avaiable.

### Linux

In linux, `auditwheel` is used for creating python wheel packages ocntains
precompiled binary extensions.
Header locations and link flags can be customized by `GAMMA_INCLUDE` and
`GAMMA_LDFLAGS` environment variables for building wheel packages.
Windows and OSX are not supported yet.
