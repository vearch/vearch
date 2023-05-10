
# Benchmarks

This README.md shows the experiments we do and the results we get. Here we do two series of experiments. First, we experiment on a single node to show the recalls of the modified IVFPQ model which is based on faiss. Second, we do experiments with Vearch cluster.

We evaluate methods with the recall at k performance measure, which is the proportion of results that contain the ground truth nearest neighbor when returning the top k candidates (for k ∈{1,10,100}). And we use Euclidean neighbors as ground truth.

Note that the numbers (especially QPS) change slightly due to changes in the implementation, different machines, etc.

## Getting data

We do experiments on two kind of features. One is 128-dimensional SIFT feature, the other is 512-dimensional VGG feature.

### Getting SIFT1M and SIFT10M

To run it, please download the ANN_SIFT1M dataset from

http://corpus-texmex.irisa.fr/

and unzip it to the subdirectory sift1M. The sift10M data was intercepted from the Base_set the in the ANN_SIFT1B.

### Getting VGG1M and VGG10M

We get 1 million and other 10 million data  and then use deep-learning model vgg to get  their features. 

### Getting VGG100M , VGG500M and VGG1B

We collect billions of data and use deep-learning model vgg to get their features for cluster experiments.

## Nprobe experiments

We do experiments on SIFT1M, VGG1M and VGG10M. In this experiment, nprobe  ∈{1,5,10,20,30,40,50,80,100,200}. At the same time, we set  the ncentroids as 256 and the nbytes as 32.

We use recall at 1 to show the result.

### Result

![nprobe](/doc/img/benchs/nprobe.png)

As we can see, when nprobe exceeds 25, there is no obvious change of recalls. Also, when nprobe get larger,only QPS of vgg10M get smaller, QPS of vgg1M and QPS of sift1M  basically have no changes.

## Ncentroids experiments

We do experiment on VGG10M. The number of centroid ∈{64,128,256,512,1024,2048,4096,8192} and we set nprobe as 50 considering the  number of centroid becomes very large. Here we also set nbytes as 32. We use recall at 1 to show the result.

### Result

![ncentroids](/doc/img/benchs/ncentroids.png)

As we can see, there is no obvious change of recalls when the number of centroid get larger. But the QPS become higher and higher as the number of centroid grows.

## Nbytes experiments

We do experiment on VGG10M. The number of byte ∈{4,8,16,32,64}. We set ncentroids as 256 and nprobe as 50. We use recall at 1 to show the result.

### Result

![nbytes](/doc/img/benchs/nbytes.png)

As we can see, when the number of byte grows, the recall get higher and higher, but the QPS drops obviously.

## Experiments with faiss

We do experiments on SIFT1M, SIFT10M, VGG1M and VGG10M to compare the recalls with faiss. We use some algorithm implemented with faiss and we use Vearch to represent our algorithm. 

### Models

Here we show the parameters we set for used models. When the parameters in the table are empty, there are no corresponding parameters in the models. And the parameters of links, efSearch and efConstruction are defined in faiss of hnsw.

|  model  | ncentroids of 1M | ncentroids of 10M | nprobe of 1M | nprobe of 10M | bytes | links | efSearch | efConstruction |
| :-----: | :--------------: | :---------------: | :----------: | ------------- | :---: | :---: | :------: | :------------: |
|   pq    |                  |                   |              |               |  64   |       |          |                |
|  ivfpq  |       1024       |       2048        |      40      | 80            |  64   |       |          |                |
|  imipq  |     2^(2*10)     |     2^(2*10)      |     2048     | 2048          |  64   |       |          |                |
| opq+pq  |                  |                   |              |               |  64   |       |          |                |
|  hnsw   |                  |                   |              |               |       |  32   |    64    |       40       |
| ivfhnsw |       1024       |       2048        |      40      | 80            |       |  32   |    64    |       40       |
| Vearch  |       1024       |       2048        |      40      | 80            |  64   |       |          |                |

### Result

recalls of SIFT1M:

|  model  | recall@1 | recall@10 | recall@100 |
| :-----: | :------: | :-------: | :--------: |
|   pq    |  0.8288  |  0.9999   |     1      |
|  ivfpq  |  0.8201  |  0.9899   |   0.9901   |
|  imipq  |  0.8344  |  0.9834   |   0.9834   |
| opq+pq  |  0.8234  |  0.9998   |     1      |
|  hnsw   |  0.9795  |  0.9872   |   0.9872   |
| ivfhnsw |  0.9825  |  0.9896   |   0.9896   |
| Vearch  |  0.9814  |  0.9902   |   0.9902   |

recalls of SIFT10M:

|  model  | recall@1 | recall@10 | recall@100 |
| :-----: | :------: | :-------: | :--------: |
|   pq    |  0.7237  |  0.9976   |     1      |
|  ivfpq  |  0.7189  |  0.9794   |   0.9811   |
|  imipq  |   0.7    |  0.9086   |   0.9091   |
| opq+pq  |  0.7512  |  0.9988   |     1      |
|  hnsw   |  0.8425  |  0.8426   |   0.8426   |
| ivfhnsw |  0.9625  |  0.9626   |   0.9626   |
| Vearch  |  0.977   |  0.9774   |   0.9774   |

recalls of VGG1M :

|  model  | recall@1 | recall@10 | recall@100 |
| :-----: | :------: | :-------: | :--------: |
|   pq    |  0.5134  |  0.9003   |   0.9939   |
|  ivfpq  |  0.5234  |  0.8916   |   0.9707   |
|  imipq  |  0.5185  |  0.8837   |   0.9420   |
| opq+pq  |  0.5269  |  0.9211   |   0.9986   |
|  hnsw   |  0.9515  |  0.9563   |   0.9563   |
| ivfhnsw |  0.9652  |  0.9704   |   0.9705   |
| Vearch  |  0.9651  |  0.9702   |   0.9703   |

recalls of VGG10M :

|  model  | recall@1 | recall@10 | recall@100 |
| :-----: | :------: | :-------: | :--------: |
|   pq    |  0.5957  |  0.9009   |   0.9906   |
|  ivfpq  |  0.6068  |  0.9014   |   0.9833   |
|  imipq  |  0.6023  |  0.8955   |   0.9605   |
| opq+pq  |  0.6102  |  0.9223   |   0.9954   |
|  hnsw   |  0.8847  |  0.9045   |   0.905    |
| ivfhnsw |  0.9617  |  0.9825   |   0.9829   |
| Vearch  |  0.9649  |  0.9829   |   0.9832   |

## Cluster experiments

First, we do experiments by searching on cluster only with vgg features. Then, we experiment with the vgg features and filter the search using an integer field to compare the time consumed and QPS with the vgg features only. In the following section, we use searching with filter or without filter to specify the experiment method mentioned earlier. For different size of experiment data, we use different Vearch cluster. We use 3 masters, 3 routers and 5 partition services for VGG100M. For VGG500M, we use the same size of master and router with VGG100M but 24 partition services. We use 3 masters, 6 routers and 48 partition services to deal with the VGG1B.

### Result

![cluster](/doc/img/benchs/cluster.png)

The growth shape of QPS is more like inverted J-shaped curve which means the growth of QPS basically have no obvious change when average latency exceed one certain number. 
