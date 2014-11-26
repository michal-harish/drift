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

Usecase 2A. Benchmark - combining datasets from id-spaces - two tables from different Keyspaces, with StreamJoin and key transformation (!) 
-------------------------------------------------------------------------------------------------------------------------------------------

<html><pre>

addthis.views    (6m, 266Mb.gz)         addthis.syncs      (1m, 32Mb.gz)        addthis.views             (2.45m, 3.02Mb)
+--------+--------+-----------+         +--------+---------------------+        +---------------------+-----------+-----+
| at_id  | url    | timestamp |         | at_id  | vdna_user_id        |        | user_uid            | timestamp | url |
+--------+--------+-----------+         +--------+---------------------+        +---------------------+-----------+-----+
| STRING | STRING | LONG      |         | STRING | UUID(BYTEARRAY[16]) |        | UUID(BYTEARRAY[16]) | LONG      |     |
+--------+--------+-----------+         +--------+---------------------+        +---------------------+-----------+-----+

+----------------------------------+---------------------+--------------+----------+--------+---------+--------+---------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 6m views (266Mb.gz)              | DRIFT4              | DRIFT3       | DRIFT2   | DRIFT1 | DRIFT0  | HBase  | SPARK   | HADOOP  | (drift or equivalent command)                                                                                                                                                |
| 1m syncs (32Mb.gz)               | (LZ4fs)             | (LZ4fs)      | (LZ4mem) | (mem)  | (LZ4fs) | (mem)  | (1.0.1) | (fs.gz) |                                                                                                                                                                              |
|                                  | Cluster[3x4]        | Cluster[1x4] | Single   | Single | Single  |        |         |         |                                                                                                                                                                              |
+----------------------------------+---------------------+--------------+----------+--------+---------+--------+---------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 1. Load Table 1 - Views          | 2m.44s              | 48s          | 41s      | 26s    | 28s     | 72.10s |         |         | time cat ~/addthis_views_2014-10-31_15.csv.gz | java -jar target/drift-loader.jar --separator '\t' --gzip --keyspace addthis --table views                                   |
|                                  | (client wifi)       |              |          |        |         |        |         |         |                                                                                                                                                                              |
+----------------------------------+---------------------+--------------+----------+--------+---------+--------+---------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 2. Load Table 2 - Syncs          | 2m.44s(client wifi) | 10s          | 8.5s     | 8.5s   | 8.5s    | 13.56s |         |         | time cat ~/addthis_syncs_2014-10-31_15.csv.gz | java -jar target/drift-loader.jar --separator '\t' --gzip --keyspace addthis --table syncs                                   |
+----------------------------------+---------------------+--------------+----------+--------+---------+--------+---------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 3. Stored Size (Mb)              | H263                | H213         | M222     | M660   | H213    | M1500  |         |         |                                                                                                                                                                              |
+----------------------------------+---------------------+--------------+----------+--------+---------+--------+---------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 4. Minimum Heap                  |                     | 64           |          |        | 32      |        |         | 0       | use addthis; stats                                                                                                                                                           |
|    Optimal Heap (Mb)             |                     | 256          |          |        | 64      |        |         |         |                                                                                                                                                                              |
+----------------------------------+---------------------+--------------+----------+--------+---------+--------+---------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 5. SCAN Syncs for a column value | 100ms               | 50ms         | 70ms     | 45ms   | 80ms    | 666ms  |         |         | select at_id,vdna_user_uid from addthis.syncs where vdna_user_uid= 'ce1e0d6b-6b11-428c-a9f7-c919721c669c'                                                                    |
+----------------------------------+---------------------+--------------+----------+--------+---------+--------+---------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 6. SCAN Views for a url contains | 200ms               | 0.5s         | 0.9s     | 0.5s   | 1s      | 5.660s |         |         | select at_id,url from addthis.views where url contains 'http://www.toysrus.co.uk/Toys-R-Us/Toys/Cars-and-Trains/Cars-and-Playsets'                                           |
+----------------------------------+---------------------+--------------+----------+--------+---------+--------+---------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 7. COUNT inner join              | 400ms               | 1.2s         | 2.6s     | 2.5s   | 2.8s    | 3.060s |         |         | count (select at_id from addthis.syncs join select at_id from addthis.views)                                                                                                 |
+----------------------------------+---------------------+--------------+----------+--------+---------+--------+---------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 8. TRANSFORM inner join          | 4.3s                | 8.2s         | 10s      | 6s     | 8.5s    |        |         |         | select vdna_user_uid from addthis.syncs join select timestamp,url from addthis.views into vdna.pageviews                                                                     |
+----------------------------------+---------------------+--------------+----------+--------+---------+--------+---------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 9. EXPORT inner join to file     | 35s                 | 45s          | 35s      | 35s    | 35s     |        |         |         | time java -jar target/drift-client.jar --keyspace addthis "select vdna_user_uid from addthis.syncs join select timestamp,url from addthis.views" > ~/vdna-addthis-export.csv |
+----------------------------------+---------------------+--------------+----------+--------+---------+--------+---------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 10. RANDOM ACCESS                |                     | N/A          | N/A      | N/A    | N/A     |        |         | N/A     |                                                                                                                                                                              |
+----------------------------------+---------------------+--------------+----------+--------+---------+--------+---------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

</pre></html>

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

Usecase 2B. Benchmark - combining datasets from id-spaces - two tables from different Keyspaces, with StreamJoin and key transformation (!) 
-------------------------------------------------------------------------------------------------------------------------------------------
<html><pre>

addthis.views  (102m, 4.1GB.gz)         addthis.syncs    (18m, 540Mb.gz)        addthis.views             (???m, ?????Mb)
+--------+--------+-----------+         +--------+---------------------+        +---------------------+-----------+-----+
| at_id  | url    | timestamp |         | at_id  | vdna_user_id        |        | user_uid            | timestamp | url |
+--------+--------+-----------+         +--------+---------------------+        +---------------------+-----------+-----+
| STRING | STRING | LONG      |         | STRING | UUID(BYTEARRAY[16]) |        | UUID(BYTEARRAY[16]) | LONG      |     |
+--------+--------+-----------+         +--------+---------------------+        +---------------------+-----------+-----+

+----------------------------------+--------------+--------------+--------------+----------+--------+---------+-------+---------+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 102m views(4.2Gb)                | DRIFT5B      | DRIFT4B      | DRIFT3       | DRIFT2   | DRIFT1 | DRIFT0  | HBase | SPARK   | HADOOP  | (drift or equivalent command)                                                                                                                                                  |
| 18m syncs (540Mb)                | (MEMLZ4)     | (MEMLZ4)     | (LZ4fs)      | (LZ4mem) | (mem)  | (LZ4fs) | (mem) | (1.0.1) | (fs.gz) |                                                                                                                                                                                |
| 24h window                       | Cluster[8x1] | Cluster[8x4] | Cluster[1x4] | Single   | Single | Single  |       |         |         |                                                                                                                                                                                |
+----------------------------------+--------------+--------------+--------------+----------+--------+---------+-------+---------+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 1. Load Table 1 - Views          |              | 7m50s        | 5m12s        |          |        |         | 50m   |         |         | time java -jar drift-loader.jar --file ~/addthis_views_2014-10-31.csv.gz --separator '\t' --gzip --keyspace addthis --table views --host bl-mharis-d02                         |
+----------------------------------+--------------+--------------+--------------+----------+--------+---------+-------+---------+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 2. Load Table 2 - Syncs          |              | 2m           | 1m10s        |          |        |         | 8m    |         |         | time java -jar drift-loader.jar --cluster-id benchmark3 --keyspace addthis --table syncs --file ~/addthis_syncs_2014-10-31.csv.gz --separator '\t' --gzip --host bl-mharis-d02 |
+----------------------------------+--------------+--------------+--------------+----------+--------+---------+-------+---------+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 3. Stored Size (Gb)              |              |              | H4.4         |          |        |         |       |         |         |                                                                                                                                                                                |
+----------------------------------+--------------+--------------+--------------+----------+--------+---------+-------+---------+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 4. Minimum Heap                  |              |              |              |          |        |         |       |         | 0       | use addthis; stats                                                                                                                                                             |
|    Optimal Heap (Mb)             |              |              | 1024         |          |        |         |       |         |         |                                                                                                                                                                                |
+----------------------------------+--------------+--------------+--------------+----------+--------+---------+-------+---------+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 5. SCAN Syncs for a column value |              | 300ms        | 600ms        |          |        |         |       |         |         | select at_id,vdna_user_uid from addthis.syncs where vdna_user_uid= 'ce1e0d6b-6b11-428c-a9f7-c919721c669c'                                                                      |
|                                  |              | [7]          | [7]          |          |        |         |       |         |         |                                                                                                                                                                                |
+----------------------------------+--------------+--------------+--------------+----------+--------+---------+-------+---------+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 6. SCAN Views for a url contains |              | 2.5s         | 10s          |          |        |         |       |         |         | select at_id,url from addthis.views where url contains 'http://www.toysrus.co.uk/Toys-R-Us/Toys/Cars-and-Trains/Cars-and-Playsets/Micro'                                       |
|                                  |              | [199]        | [199]        |          |        |         |       |         |         |                                                                                                                                                                                |
+----------------------------------+--------------+--------------+--------------+----------+--------+---------+-------+---------+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 7. COUNT inner join              |              | 5s           | 26s          |          |        |         |       |         |         | count (select at_id from addthis.syncs join select at_id from addthis.views)                                                                                                   |
|                                  |              | [50723673]   |              |          |        |         |       |         |         |                                                                                                                                                                                |
+----------------------------------+--------------+--------------+--------------+----------+--------+---------+-------+---------+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 8. TRANSFORM inner join          |              | ?            | 4m           |          |        |         |       |         |         | select vdna_user_uid from addthis.syncs join select timestamp,url from addthis.views into vdna.pageviews                                                                       |
|                                  |              | [?]          | [50723649]   |          |        |         |       |         |         |                                                                                                                                                                                |
+----------------------------------+--------------+--------------+--------------+----------+--------+---------+-------+---------+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 9. EXPORT inner join to file     |              |              | 12m          |          |        |         |       |         |         | time java -jar target/drift-client.jar --keyspace addthis "select vdna_user_uid from addthis.syncs join select timestamp,url from addthis.views" > ~/vdna-addthis-export.csv   |
|                                  |              |              | [50723876]   |          |        |         |       |         |         |                                                                                                                                                                                |
+----------------------------------+--------------+--------------+--------------+----------+--------+---------+-------+---------+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 10. RANDOM ACCESS                |              |              | N/A          | N/A      | N/A    | N/A     |       |         | N/A     |                                                                                                                                                                                |
+----------------------------------+--------------+--------------+--------------+----------+--------+---------+-------+---------+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

</pre></html>

* bl-yarnpoc-p01 hive> select count(1) from hcat_addthis_raw_view_gb where d='2014-10-31'
* bl-yarnpoc-p01 ~> hive -e "select uid, url, timestamp from hcat_addthis_raw_view_gb where d='2014-10-31'" | gzip --stdout > addthis_views_2014-10-31.csv.gz
* bl-yarnpoc-p01 ~> gzip addthis_views_2014-10-31.csv
* scp mharis@bl-yarnpoc-p01:~/addthis_views_2014-10-31.csv.gz ~/

* bl-yarnpoc-p01 hive> select count(1) from hcat_events_rc where d='2014-10-31' and partner_id_space='at_id' and topic='datasync';
* bl-yarnpoc-p01 ~> hive -e "select partner_user_id, useruid, concat(timestamp,'000') from hcat_events_rc where d='2014-10-31' and partner_id_space='at_id' and topic='datasync'" > addthis_syncs_2014-10-31.csv
* bl-yarnpoc-p01 ~> gzip addthis_syncs_2014-10-31.csv
* scp mharis@bl-yarnpoc-p01:~/addthis_syncs_2014-10-31.csv.gz ~/

Usecase 3. Benchmark - id-linking from newly discovered information (?) 
---------------------------------------------------------------------------------------------------------------------------------------

Design thoughts dump
================================================================================================= 
* atm cluster doesn't have any replication and since partitioning is critical, the cluster suspends all operations if any one of the expected nodes is missing until it reappears
* compaction - use transformation method to do the merge sorting into larger and larger segments - e.g. start with tiny segments with quick sort and use maturing background transformations
* 64-bit DOUBLE AimType must be added
* Compare Memory Mapped Files on SSD vs  spinning disk storage
* some kind of consumption status of transformers at the segment level would be nice
* co-location of drift data and YARN container is worth exploring (e.g. giraph algo interlaced with drift data) 
* local cluster of 12 with fs storage seen crashing the heap but it shouldn't so needs mem-profiling
* need to get rid of 'empty table' exception and return vlaid 0-length record stream
* all counts should be refactored from Int to Long 
* the whole thing is sequential, there is no random-access whatsoever
* fine-tuning load strategies, e.g.switching between heap-sort, quick-sort when closing a segment
* fine-tuning select streams, e.g. if we want recent data we'll wait a bit longer for the initial response
* range queries are filters that limit the segments used in the consequent select stream
* design of each logical table will have : a) at the cluster level design b) schema level c) segment level d) query tuning
* We should try to wrap segement processing units into cpu-bound threads (may require C programming) 
    -> https://github.com/peter-lawrey/Java-Thread-Affinity
* Think about UTF8 column type before it's too late (everything is just a byte array atm)
* Multiple records are grouped into Segments with blockstorage for each column
* Segments of the same table can live across different nodes
* Each segment muss be pre-sorted at load-time by a configurable sort method and multiple segments are sorted with merge-sort at query time
* All columns must be present in each segment for sophisticated mapping, i.e. columns cannot be stored across multiple locations
* Each segment is stored as an LZ4-compressed in-memory buffer, therefore records cannot be addressed individually but can be 
  stored efficiently in memory and thus queried and filtered at very high speeds
* All the communication storage and streaming is compressed with LZ4 High Compression algorithm.
* The protocol uses BIG_ENDIAN format as it is better suited for stream filtering 
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



Quick-Start
===========

Example 1)  building a cluster from scratch
-------------------------------------------
cd scala
mvn package
java -jar target/drift-cluster.jar --cluster-id test1 --num-nodes 4
java -jar target/drift-client.jar 'CLUSTER numNodes 4'
#TODO create statment
java -jar target/drift-client.jar 'CREATE TABLE addthis.views at_id(STRING), url(STRING), timestamp(LONG) WITH STORAGE=LZ4, SEGMENT_SIZE=50000000'
java -jar target/drift-client.jar 'CREATE TABLE addthis.syncs at_id(STRING), vdna_user_uid(UUID), timestamp(LONG) WITH STORAGE=LZ4, SEGMENT_SIZE=200000000'
java -jar target/drift-client.jar 'CREATE TABLE vdna.pageviews user_uid(UUID), timestamp(LONG), url(STRING) WITH STORAGE=LZ4, SEGMENT_SIZE=100000000'
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


