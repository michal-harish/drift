working name: AIM
candidate name: DRIFT
the next best alternative for the basic use case: HBase

Fast sequential processing of keyed data windows - a conceptual child of Cassandra, Spark and Kafka

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


Design Concepts
===============================
 Type       (physical and logical data types)
   |
Schema      (definition of columns and KEY each having to be of one of the provided Types)
   |
Table       (a logical entity which conforms to a given schema, with sequential interface, consiting of partitions)
   |
Partition   (a physical node 
   |
Group
   |
Segment
   |
BlockStorage

![Design Overview](https://dl.dropboxusercontent.com/u/15048579/drift.svg "Design Overview")

Usecase 1. Benchmark - retroactive data windows: Solution - two tables in the same Keyspace, i.e. co-partitioned, with ScanJoin select 
---------------------------------------------------------------------------------------------------------------------------------------

Usecase 2. Benchmark - combining datasets from id-spaces - two tables from different Keyspaces, with StreamJoin and key transformation (!) 
-------------------------------------------------------------------------------------------------------------------------------------------

views       (6m rows, 266Mb.gz)         syncs         (1m syncs 32Mb.gz)
+--------+--------+-----------+         +--------+---------------------+
| at_id  | url    | timestamp |         | at_id  | vdna_user_id        |
+--------+--------+-----------+         +--------+---------------------+
| STRING | STRING | LONG      |         | STRING | UUID(BYTEARRAY[16]) |
+--------+--------+-----------+         +--------+---------------------+

+----------------------------------+----------+---------+---------+--------+-------+-------+--------+
| 6m views (266Mb.gz)              | DRIFT    | DRIFT   | DRIFT   | HBase  | SPARK | REDIS | HADOOP |
| 1m syncs (32Mb.gz)               | (LZ4mem) | (mem)   | (LZ4fs) | (mem)  |       |       |        |
+----------------------------------+----------+---------+---------+--------+-------+-------+--------+
| 1. Load Table 1 - Views          | 71.440s  | 39.802s |         | 72.10s |       |       | 0      |
+----------------------------------+----------+---------+---------+--------+-------+-------+--------+
| 2. Load Table 2 - Syncs          | 15.552s  | 13.01s  |         | 13.56s |       |       | 0      |
+----------------------------------+----------+---------+---------+--------+-------+-------+--------+
| 3. Memory Used                   |          |         |         |        |       |       | 0      |
+----------------------------------+----------+---------+---------+--------+-------+-------+--------+
| 4. Memory Occupied               | 222Mb    | 660Mb   |         | 1500Mb |       |       | 0      |
+----------------------------------+----------+---------+---------+--------+-------+-------+--------+
| 5. SCAN Syncs for a column value | 99ms     | 40ms    |         | 666ms  |       |       |        |
+----------------------------------+----------+---------+---------+--------+-------+-------+--------+
| 6. SCAN Views for a url contains | 1.491s   |         |         | 5.660s |       |       |        |
+----------------------------------+----------+---------+---------+--------+-------+-------+--------+
| 7. COUNT inner join              | 1.659s   |         |         | 3.060s |       |       |        |
+----------------------------------+----------+---------+---------+--------+-------+-------+--------+
| 8. TRANSFORM inner join          | 22.197s  |         |         | 0      |       |       |        |
+----------------------------------+----------+---------+---------+--------+-------+-------+--------+
| 9. EXPORT inner join to file     | 33.296s  |         |         |        |       |       |        |
+----------------------------------+----------+---------+---------+--------+-------+-------+--------+
| 10. RANDOM ACCESS                | N/A      | N/A     |         |        |       |       | N/A    |
+----------------------------------+----------+---------+---------+--------+-------+-------+--------+

DRIFT - commands to run the benchmark
1. cat ~/addthis_views_2014-10-31_15.csv.gz | java -jar target/drift-loader.jar --separator '\t' --gzip --keyspace addthis --table views
2. time cat ~/addthis_syncs_2014-10-31_15.csv.gz | java -jar target/drift-loader.jar --separator '\t' --gzip --keyspace addthis --table syncs
5. select at_id,vdna_user_uid from syncs where vdna_user_uid= 'ce1e0d6b-6b11-428c-a9f7-c919721c669c'
6. select at_id,url from views where url contains 'http://www.toysrus.co.uk/Toys-R-Us/Toys/Cars-and-Trains/Cars-and-Playsets'
count the equi inner join between both tables: count (select at_id from syncs join select at_id from views)
export crossed data into local file: time java -jar target/drift-client.jar --keyspace addthis "select vdna_user_uid, timestamp,url FROM ( select vdna_user_uid, at_id from syncs join select at_id,timestamp,url from views)" > ~/vdna-addthis-export.csv
export crossed data into vdna keyspace: time java -jar target/drift-client.jar --keyspace addthis "select vdna_user_uid from syncs join select timestamp,url from views" | java -jar target/drift-loader.jar --separator '\t' --keyspace vdna --table pageviews

BENCHMARK DATA 
#hive add this views  2014-10-31 15:00-16:00 (6 million records) (0.63Gb uncompressed) (0.22Gb compressed)
bl-yarnpoc-p01 hive> select count(1) from hcat_addthis_raw_view_gb where d='2014-10-31' and timestamp>=1414767600000 and timestamp<1414771200000;
bl-yarnpoc-p01 ~> hive -e "select uid, url, timestamp from hcat_addthis_raw_view_gb where d='2014-10-31' and timestamp>=1414767600000 and timestamp<1414771200000;" > addthis_views_2014-10-31_15.csv
bl-yarnpoc-p01 ~> gzip addthis_views_2014-10-31_15.csv
scp mharis@bl-yarnpoc-p01:~/addthis_views_2014-10-31_15.csv.gz ~/

#hive add this syncs  2014-10-31 15:00-16:00
bl-yarnpoc-p01 hive> select count(1) from hcat_events_rc where d='2014-10-31' and partner_id_space='at_id' and topic='datasync' and timestamp>=1414767600 and timestamp<1414771200;
bl-yarnpoc-p01 ~> hive -e "select partner_user_id, useruid, concat(timestamp,'000') from hcat_events_rc where d='2014-10-31' and partner_id_space='at_id' and topic='datasync' and timestamp>=1414767600 and timestamp<1414771200" > addthis_syncs_2014-10-31_15.csv
bl-yarnpoc-p01 ~> gzip addthis_syncs_2014-10-31_15.csv
scp mharis@bl-yarnpoc-p01:~/addthis_syncs_2014-10-31_15.csv.gz ~/


Usecase 3. Benchmark - id-linking from newly discovered information (?) 
---------------------------------------------------------------------------------------------------------------------------------------

Design thoughts dump
================================================================================================= 
* co-partitioning to solve brut-forcie
* by having partitions as defined above, the future map-reduce protocol is possible in principle, but until then at least something like parallel off-load to hdfs should be possible 
* because the whole thing is sequential, storage could be a choice from memory, memory-mapped files, to hard files
* fine-tuning load strategies, e.g.switching between heap-sort, quick-sort when closing a segment
* fine-tuning select streams, e.g. if we want recent data we'll wait a bit longer for the initial response
* range queries are filters that limit the segments used in the consequent select stream
* design of each logical table will have : a) at the cluster level design b) schema level c) segment level d) query tuning
* We should try to wrap segement processing units into cpu-bound threads (may require C programming) 
    -> https://github.com/peter-lawrey/Java-Thread-Affinity
* Think about UTF8 column type before it's too late (everything is just a byte array atm)
* We'll surely need a 64-bit DOUBLE too
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


Benchmark
======================
1.000.000 real events loaded into 10 segments on a single box -> 240Mb compressed to 24Mb
count(user_quizzed=true and api_key contains 'mirror')
result = 347 out of 1000000
1 CPU avg consistent query time: 113 ms -> 240Mb(lz4)/sec/box
2 CPUs: avg consistent query time: 61 ms -> 480Mb(lz4)/sec/box


Quick-Start
===========
cd java
mvn clean package
...TODO add quickstart for loading test json data

