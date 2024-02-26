# Introduction

The current directory cantains some internal tools for developers, and is not open to public.

## perf

A tool for testing gamma engine's performance. As shown below, it only need one configuration file to run "perf".

```
Usage: ./tools/perf conf_path
```

The description of each parameter in configuration file:

* add_profile_file: the profile file path which's data will be add to gamma engine.
* add_feature_file: the feature(vector) file path which's data will be add to gamma engine.
* search_feature_file: the feature file path which's data will be used to search from gamma engine.
* `fixed_search_threads: If this parameter is set, the number of search threads is fixed to the value of this parameter, otherwise the number of search threads increases gradually until QPS no longer increases, eg [1, 5, 10, 20, 30, 50, 70, 100].`
* nadd: the number of doc added to gamma
* nsearch: the number of doc searched by each thread


```JSON
{
  "add_profile_file": "./profile_1w.txt",
  "add_feature_file": "./feat_1w.dat",
  "search_feature_file": "./feat_1w.dat",
  "fixed_search_threads": 10,
  "nprobe": 10,
  "ncentroids": 256,
  "nsubvector": 64,
  "dimension": 512,
  "max_doc_size": 1000000,
  "nadd": 100000,
  "nsearch": 10000,
  "store_type": "Mmap",
  "store_param": "{\"cache_size\": 1024}",
  "has_rank": 1
}
```
