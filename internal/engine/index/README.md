# Vector indexes
Gamma is designed to support all kinds of ANN retrieval models, and each retrieval model may has its own spcific index. Besides effective similarity search, each index should provide the methods of real time adding/deleting vectors(updating can be divided into deleting and adding).

# How to integrate new indexes

## Tests

1: compare to faiss or other origin index
test_recall_baseline.py

2: benchmark for new index
test_vector_index_new_index.py

3: create for new index
test_module_space.py

4: search for new index
test_document_search.py

maybe more test