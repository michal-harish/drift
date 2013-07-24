AIM - analytics in memory or CASSPAR - a mixutre of Cassandra, Spark and Kafka

Motivation
==========
* Hadoop suffers from extensive I/O and although Map-Reduce is a good paradigm, data location must be exploited 
* Cassandra is difficult to process sequentially and is slow to read anyway
* Streaming approach is important as data grows but the most valuable computations are derived from larger data windows
  -> "continuous" filtering / mapping / hashing is necessary to avoid costly reductions
* All data in real-world applications have TTL which can span months so Kafka segmentation approach seems reasonable 
  instead of cassandra row-level ttl but very fast querying must be possible 
  -> so segments must have a columnar format allow for high speed filtering without re-streaming the data to a remote process 
  -> sending the mappers to the data rather then loading the data and then mapping like in Hadoop or Spark 
  -> reducing will be limited but can be compensated with a Large HashTable format

Architecture Decisions Overview
===============================
 
* Table is a virtual entity which has strictly-typed structure of columns and records
* Each record must be stored in a single location for sophisticated mapping, i.e. columns cannot be stored across multiple locations
* Multiple records are stored together in an Segment, which can be distributed across different nodes
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
* Trivial binary protocol with transparnt LZ4 streaming compression is used, with following data types:

    ID  Data Type      Size(bytes)
    ...............................
    1   BOOL            1
    2   BYTE            1
    3   INT             4
    4   LONG            8
    5   STRING          4 + n
    6   BYTEARRAY(N)    N
  
Architecture TODOs and NOTEs
============================
- We should try to wrap segement processing units into cpu-bound threads (may require C programming) 
- We need to compensate reduction with Hashtable 


Protocol Message Types
======================

Schema
------

Filter BitSet 
-------------

Select Stream
-------------


 
