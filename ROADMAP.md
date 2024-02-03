# Roadmap

The roadmap provides a high level overview of key areas that will likely span multiple releases.

## Vearch 0.1

0.1 was released in early September.

### Usage

End-to-End one-click deployment.

RESTful API 

### Index & Search

Support L2 and inner-product searching metric.

Replace the static batch indexing with real time indexing.

Add the fine-grained sort after PQ coarse sort.

Add the numeric field and bitmap filters in the process of searching.

### Models

Real-time modified IVFPQ model based on Faiss.

### Storage

Only memory supported now.

### Container

Support docker.

## Vearch 0.2

Vearch 0.2 has been released on October 31th 2019.

### Search

Numeric index filtering optimization.

### Storage

Memory and disk supported now.

### Plug-in

Video surveillance security scene algorithm plug-in.

## Vearch 0.3

Vearch 0.3 released on 20th January 2020.

### New features 

Support vector search with GPU. Support single online request for GPU, not just batch requests

Python SDK which can be friendly used in local computer or edge device.

### Plug-in

Plugin service can be installed in Docker.

### Testing

Stability testing.

## Vearch 3.1.0

Vearch 3.1.0 released in May 2020.

### New features

Support real time HNSW index

Support binary index

Support IVFFLAT index

## Vearch 3.2.0

Vearch 3.2.0 will be released in August 2020.

### New features

Clear gamma engine api

Gamma engine supports customerized retrieval models

Support the Grpc service for router

Refactor for the router and partition server


## Vearch 4.0 in 2024

* decoupled compute and storage
* hybrid search
* multi-tenancy


