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

<html><pre>

addthis.views    (6m, 266Mb.gz)         addthis.syncs      (1m, 32Mb.gz)        addthis.views             (2.45m, 3.02Mb)
+--------+--------+-----------+         +--------+---------------------+        +---------------------+-----------+-----+
| at_id  | url    | timestamp |         | at_id  | vdna_user_id        |        | user_uid            | timestamp | url |
+--------+--------+-----------+         +--------+---------------------+        +---------------------+-----------+-----+
| STRING | STRING | LONG      |         | STRING | UUID(BYTEARRAY[16]) |        | UUID(BYTEARRAY[16]) | LONG      |     |
+--------+--------+-----------+         +--------+---------------------+        +---------------------+-----------+-----+

+----------------------------------+-------------+------------------+------------------+-----------+-------------+-----------+-----------+-------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 6m views (266Mb.gz)              | DRIFT6      | DRIFT4           | DRIFT3           | DRIFT2    | DRIFT1      | DRIFT0    | HBase     | SPARK | HADOOP  | (drift or equivalent command)                                                                                                                                                |
| 1m syncs (32Mb.gz)               | (FSLZ4)     | (MEMLZ4)         | (FSLZ4)          | (MEMLZ4)  | (MEM)       | (FSLZ4)   | (MEM)     | (MEM) | (fs.gz) |                                                                                                                                                                              |
|                                  | 3 x 4 CPU   | 4 CPU Cluster[4] | 4 CPU Cluster[4] | 4 CPU     | 4 CPU Local | 4 CPU     |           |       |         |                                                                                                                                                                              |
|                                  | Cluster[12] |                  |                  | Local     |             | Local     |           |       |         |                                                                                                                                                                              |
+----------------------------------+-------------+------------------+------------------+-----------+-------------+-----------+-----------+-------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 1. Load Table 1 - Views          |             | 74s              | 75s              | 60s       | 38s         | 62s       | 72.10s    |       |         | time cat ~/addthis_views_2014-10-31_15.csv.gz | java -jar target/drift-loader.jar --separator '\t' --gzip --keyspace addthis --table views                                   |
+----------------------------------+-------------+------------------+------------------+-----------+-------------+-----------+-----------+-------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 2. Load Table 2 - Syncs          |             | 13s              | 12s              | 12s       | 9s          | 11s       | 13.5s     |       |         | time cat ~/addthis_syncs_2014-10-31_15.csv.gz | java -jar target/drift-loader.jar --separator '\t' --gzip --keyspace addthis --table syncs                                   |
+----------------------------------+-------------+------------------+------------------+-----------+-------------+-----------+-----------+-------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 3. Query-Time Extra Memory Used  |             | 2Mb              | 16Mb             | 512Kb     | 1Mb         | 4Mb       | ?         |       |         |                                                                                                                                                                              |
+----------------------------------+-------------+------------------+------------------+-----------+-------------+-----------+-----------+-------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 4. Store (Mb)RAM                 |             | 222              | 0                | 222       | 660         | 0         | 1500      |       | 0       | stats                                                                                                                                                                        |
|              (Mb)HDD             |             | 0                | 213              | 0         | 0           | 213       | ?         |       | ?       | du -hd 1 /var/lib/drift/                                                                                                                                                     |
+----------------------------------+-------------+------------------+------------------+-----------+-------------+-----------+-----------+-------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 5. SCAN Syncs for a column value |             | 50ms             | 300ms/80ms       | 90ms      | 75ms        | 120ms     | 666ms     |       |         | select at_id,vdna_user_uid from addthis.syncs where vdna_user_uid= 'ce1e0d6b-6b11-428c-a9f7-c919721c669c'                                                                    |
|                                  |             |                  | [4]              | [4]       | [4]         |           |           |       |         |                                                                                                                                                                              |
|                                  |             |                  |                  |           |             |           |           |       |         |                                                                                                                                                                              |
+----------------------------------+-------------+------------------+------------------+-----------+-------------+-----------+-----------+-------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 6. SCAN Views for a url contains |             | 700ms            | 1s               | 1.25s     | 700ms       | 1.6s      | 5.660s    |       |         | select at_id,url from addthis.views where url contains 'http://www.toysrus.co.uk/Toys-R-Us/Toys/Cars-and-Trains/Cars-and-Playsets'                                           |
|                                  |             | [158]            | [158]            | [158]     | [158]       | [158]     | [158]     |       |         |                                                                                                                                                                              |
|                                  |             |                  |                  |           |             |           |           |       |         |                                                                                                                                                                              |
+----------------------------------+-------------+------------------+------------------+-----------+-------------+-----------+-----------+-------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 7. COUNT inner join              |             | 900ms            | 1s               | 4s        | 3.6s        | 4.3s      | 3.060s    |       |         | count (select at_id from addthis.syncs join select at_id from addthis.views)                                                                                                 |
|                                  |             | [2456445]        | [2456445]        | [2456443] | [2456443]   | [2456443] | [2456141] |       |         |                                                                                                                                                                              |
+----------------------------------+-------------+------------------+------------------+-----------+-------------+-----------+-----------+-------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 8. TRANSFORM inner join          |             | 10s              | 10s              | 18s       | 8s          | 18s       |           |       |         | select vdna_user_uid from syncs join select timestamp,url from views into vdna.pageviews                                                                                     |
|                                  |             | [2456443]        | [2456443]        | [2456443] | [2456443]   | [2456443] |           |       |         |                                                                                                                                                                              |
+----------------------------------+-------------+------------------+------------------+-----------+-------------+-----------+-----------+-------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 9. EXPORT inner join to file     |             | 38s              | 38s              | 23s       | 23s         | 22s       |           |       |         | time java -jar target/drift-client.jar --keyspace addthis "select vdna_user_uid from addthis.syncs join select timestamp,url from addthis.views" > ~/vdna-addthis-export.csv |
+----------------------------------+-------------+------------------+------------------+-----------+-------------+-----------+-----------+-------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 10. RANDOM ACCESS                |             | N/A              | N/A              | N/A       | N/A         | N/A       |           |       | N/A     |                                                                                                                                                                              |
+----------------------------------+-------------+------------------+------------------+-----------+-------------+-----------+-----------+-------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

* hive add this views  2014-10-31 15:00-16:00 (6 million records) (0.63Gb uncompressed) (0.22Gb compressed)
* bl-yarnpoc-p01 hive> select count(1) from hcat_addthis_raw_view_gb where d='2014-10-31' and timestamp>=1414767600000 and timestamp<1414771200000;
* bl-yarnpoc-p01 ~> hive -e "select uid, url, timestamp from hcat_addthis_raw_view_gb where d='2014-10-31' and timestamp>=1414767600000 and timestamp<1414771200000;" > addthis_views_2014-10-31_15.csv
* bl-yarnpoc-p01 ~> gzip addthis_views_2014-10-31_15.csv
* scp mharis@bl-yarnpoc-p01:~/addthis_views_2014-10-31_15.csv.gz ~/

hive add this syncs  2014-10-31 15:00-16:00
* bl-yarnpoc-p01 hive> select count(1) from hcat_events_rc where d='2014-10-31' and partner_id_space='at_id' and topic='datasync' and timestamp>=1414767600 and timestamp<1414771200;
* bl-yarnpoc-p01 ~> hive -e "select partner_user_id, useruid, concat(timestamp,'000') from hcat_events_rc where d='2014-10-31' and partner_id_space='at_id' and topic='datasync' and timestamp>=1414767600 and timestamp<1414771200" > addthis_syncs_2014-10-31_15.csv
* bl-yarnpoc-p01 ~> gzip addthis_syncs_2014-10-31_15.csv
* scp mharis@bl-yarnpoc-p01:~/addthis_syncs_2014-10-31_15.csv.gz ~/


Usecase 3. Benchmark - id-linking from newly discovered information (?) 
---------------------------------------------------------------------------------------------------------------------------------------

Design thoughts dump
================================================================================================= 
* need to get rid of 'empty table' exception and return vlaid 0-length record stream
* all counts should be refactored from Int to Long
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
  * Filter - produces a FilterBitSet for given range (set of segments)
  *  Select - doesn't have a where condition but is given the FilterBitSet  
* Components: Loaders -> Loading Interface -> Storage <- Filter Interface <- Select Interface 
  *  Loading is done using IPC and is only defined by a protocol so any language can be used 
  * Memory Mapped Files are used for storage 
  *  Go , C and other low-level languages can be used to implement the central component
  * Filter and Select Interfaces should be done in Scala as this allows for passing mapper/select code around nicely
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


Example 1)  building a cluster from scratch
-------------------------------------------
cd scala
mvn package
java -jar target/drift-cluster.jar --root drift-test1 --num-nodes 4
java -jar target/drift-client.jar 'CLUSTER NUM NODES 4'
#TODO create statment
java -jar target/drift-client.jar 'CREATE TABLE addthis.views at_id(STRING), url(STRING), timestamp(LONG) WITH STORAGE=LZ4, SEGMENT_SIZE=50000000'
java -jar target/drift-client.jar 'CREATE TABLE addthis.syncs at_id(STRING), vdna_user_uid(UUID:BYTEARRAY[16]), timestamp(LONG) WITH STORAGE=LZ4, SEGMENT_SIZE=200000000'
java -jar target/drift-client.jar 'CREATE TABLE vdna.pageviews user_uid(UUID:BYTEARRAY[16]), timestamp(LONG), url(STRING) WITH STORAGE=LZ4, SEGMENT_SIZE=100000000'
time cat ~/addthis_syncs_2014-10-31_15.csv.gz | java -jar target/drift-loader.jar --separator '\t' --gzip --keyspace addthis --table syncs
time cat ~/addthis_views_2014-10-31_15.csv.gz | java -jar target/drift-loader.jar --separator '\t' --gzip --keyspace addthis --table views


Example 2) continuous-loading of keyspace from vdna events 
----------------------------------------------------------

echo '~/kafka/bin/kafka-console-consumer.sh --zookeeper zookeeper-04.prod.visualdna.com $@' > ./kafka8 && chmod a+x ./kafka8

#test datasync 
cat src/test/resources/datasync.json \
| jq -r 'select(.partnerUserId!=null and .userUid!=null) | .userUid,.timestamp,.idSpace,.partnerUserId' \
| java -jar target/drift-loader.jar --keyspace vdna --table syncs

#stream of syncs
~/kafka8 --topic datasync \
| jq -r 'select(.partnerUserId!=null and .userUid!=null) | .userUid,.timestamp,.idSpace,.partnerUserId' \
| java -jar target/drift-loader.jar --keyspace vdna --table syncs

#test pageviews
cat src/test/resources/pageviews.json \
| jq -r 'select(.userUid!=null) | .userUid,.timestamp,.type,.url' \
| java -jar target/drift-loader.jar --keyspace vdna --table events

#stream of pageviews
~/kafka8 --topic pageviews \
| jq -r 'select(.userUid!=null and .type!=null) | .userUid,.timestamp,.type,.url' \
| java -jar target/drift-loader.jar --keyspace vdna --table events


