AIM - crossing large data sets in memory - a conceptual child of Cassandra, Spark and Kafka


Motivation
==========
* Hadoop suffers from extensive I/O and although Map-Reduce is a good paradigm, data location must be exploited better 
* Cassandra is difficult to process sequentially and is slow to read anyway
* Streaming approach is important as data grows but the most valuable computations are derived from larger data windows
  -> "continuous" filtering / mapping / hashing is necessary to avoid costly reductions
* All data in real-world applications have TTL which can span months so Kafka segmentation approach seems reasonable 
  instead of cassandra row-level ttl but very fast sequential querying must be possible 
  -> so segments must have a columnar format allow for high speed filtering without re-streaming the data to a remote process 
  -> sending the mappers to the data rather then loading the data and then mapping like in Hadoop or Spark

Usecases
===========

1) mixing pageview datasets from multiple id-spaces, e.g. addthis + visualdna pageviews
2) identity linking from newly discovered information
3) ...

Design Overview and Decisions
===============================

![Design Overview](https://dl.dropboxusercontent.com/u/15048579/aim.svg "Design Overview")
 
* Table is a virtual entity which has a strictly-typed structure of columns, however individual records are not randomly accessible
* Multiple records are stored together in an Segment, which can be distributed across different nodes
* All columns must be present in each segment for sophisticated mapping, i.e. columns cannot be stored across multiple locations
* Each segment is stored as an LZ4-compressed in-memory buffer, therefore records cannot be addressed individually but can be 
  stored efficiently in memory and thus queried and filtered at very high speeds
* All the communication storage and streaming is compressed with LZ4 High Compression algorithm.
* Standard method of querying is replaced 2 separate processes: 
  1. Filter - produces a FilterBitSet for given range (set of segments)
  2. Select - doesn't have a where condition but is given the FilterBitSet  
* Components: Loaders -> Loading Interface -> Storage <- Filter Interface <- Select Interface 
  - Loading is done using IPC and is only defined by a protocol so any language can be used 
  - Memory Mapped Files are used for storage 
  - Go , C and other low-level languages can be used to implement the central component
  - Filter and Select Interfaces should be done in Scala as this allows for passing mapper/select code around nicely
* The protocol uses BIG_ENDIAN format as it is better suited for filtering comparsions 
* Trivial binary protocol with transparent LZ4 streaming compression is used, with following data types:
* Each segment can be sorted at load-time with quick-sort and multiple segments are sorted with merge-sort at query time

    ID  Data Type      Size(bytes)      Description
    ...................................................................................................
    1   BOOL            1               true or false boolean stored as byte
    2   BYTE            1               unsigned byte
    3   INT             4               32-bit signed integer
    4   LONG            8               64-bit unsigned long
    5   STRING          4 + ?           string of bytes with dynamic length stored as first 32-bits
    6   STRING[N]       N               fixed length string of bytes
    7-20   -            -               reserved


Quick-Start
===========
cd java
mvn clean package
...TODO add quickstart for loading test json data


TODOs and NOTEs
============================
- We should try to wrap segement processing units into cpu-bound threads (may require C programming) 
    -> https://github.com/peter-lawrey/Java-Thread-Affinity
- We need to compensate reduction with Hashtable or some clever Indexing mechanism
- Think about UTF8 column type before it's too late (everything is just a byte array atm)
- We'll surely need a 64-bit DOUBLE too
- Think about custom column types like UUID[16] IPV4[4] IPV6[6] UTC[4] or schema-mapping functions
    - shema-mapping functions is cleaner as it is only in the wolrd of loaders 
    - but then the filters still need to do the same so probably custom fields

Benchmark
======================
1.000.000 real events loaded into 10 segments on a single box -> 240Mb compressed to 24Mb
count(user_quizzed=true and api_key contains 'mirror')
result = 347 out of 1000000
1 CPU avg consistent query time: 113 ms -> 240Mb(lz4)/sec/box
2 CPUs: avg consistent query time: 61 ms -> 480Mb(lz4)/sec/box

Protocol Message Types
======================

Schema
------

Filter BitSet 
-------------

Select Stream
-------------

