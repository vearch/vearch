Summary
========

Vearch is a scalable distributed system for efficient similarity search of deep learning vectors.

Overall Architecture
-----------------------

.. image:: pic/vearch-arch.png
   :align: center
   :scale: 50 %
   :alt: Architecture

Data Model: space, documents, vectors, scalars

Components: Master，Routerm，PartitionServer。

Master: Responsible for schema mananagement, cluster-level metadata, and resource coordination.

Router: Provides RESTful API: create , delete search and update ; request routing, and result merging.

PartitionServer(PS): Hosts document partitions with raft-based replication. Gamma is the core vector search engine. It provides the ability of storing, indexing and retrieving the vectors and scalars.


General Introduction
-----------------------

1. One document one vector.

2. One document multiple vectors.

3. One document has multiple data sources and vectors.

4. Numerical field filtration

5. Batch operations to support addition and search.


System Features
-----------------------
1. Gamma engine implemented by C++ guarantees fast detection of vectors.

2. Supporting Interior Product and L2 Method to Calculate Vector Distance.

3. Supporting memory and disk data storage, supporting super-large data scale.

4. Data multi copy storage based on raft protocol.

