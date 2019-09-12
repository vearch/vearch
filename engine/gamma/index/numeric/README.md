# numeric range index #
This module is designed to do the numeric range filter (as the below) fast.

    "filter": [
        "category": {"eq": 655},
        "price": {"gt": 10, "lt": 20}
    ]

For performance, two index types are designed:

1. block-skiplist index
2. rt-skiplist index

The both are implemented based on a variant skiplist index.

## block-skiplist Index ##
It's designed to support the full indexing and searching.

![Block-Skiplist Index](/docs/img/gamma/block-skiplist_index.png)

## rt-skiplist Index ##
It's designed to support the real-time indexing and searching.

![rt-Skiplist Index](/docs/img/gamma/rt-skiplist_index.png)

## about search ##
The main search steps are:

1. search in the block-skiplist index.
2. search in the rt-skiplist index.
3. build a bitmap result while 1 & 2.

## performance ##
Based on a random 10 million *dense* data set, a simple benchmark will be given here.
