# Vector indexes
Gamma is designed to support all kinds of ANN retrieval models, and each retrieval model may has its own spcific index. Besides effective similarity search, each index should provide the methods of real time adding/deleting vectors(updating can be divided into deleting and adding).
Now the retrieval model of IVFPQ modified based on faiss has just been implemented. Other retrieval models will be added later. 
