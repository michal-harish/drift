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
* The protocol uses BIG_ENDIAN format as it is better suited for filtering comparsions 
* Trivial binary protocol with transparent LZ4 streaming compression is used, with following data types:

    ID  Data Type      Size(bytes)      Description
    ...................................................................................................
    1   BOOL            1               true or false boolean stored as byte
    2   BYTE            1               unsigned byte
    3   INT             4               32-bit signed integer
    4   LONG            8               64-bit unsigned long
    5   STRING          4 + ?           string of bytes with dynamic length stored as first 32-bits
    6   STRING[N]       N               fixed length string of bytes
    7-20   -            -               reserved
å
Architecture TODOs and NOTEs
============================
- We should try to wrap segement processing units into cpu-bound threads (may require C programming) 
- We need to compensate reduction with Hashtable or some clever Indexing mechanism
- Think about UTF8 column type before it's too late (everything is just a byte array atm)
- We'll surely need a 64-bit DOUBLE too
- Think about custom column types like UUID[16] IPV4[4] IPV6[6] UTC[4] or schema-mapping functions
    - shema-mapping functions is cleaner as it is only in the wolrd of loaders 
    - but then the filters still need to do the same so probably custom fields


Protocol Message Types
======================

Schema
------

Filter BitSet 
-------------

Select Stream
-------------


 
