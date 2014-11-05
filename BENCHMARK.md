1. Benchmark 
============================================================================================

views       (6m rows, 266Mb.gz)         syncs         (1m syncs 32Mb.gz)
+--------+--------+-----------+         +--------+---------------------+
| at_id  | url    | timestamp |         | at_id  | vdna_user_id        |
+--------+--------+-----------+         +--------+---------------------+
| STRING | STRING | LONG      |         | STRING | UUID(BYTEARRAY[16]) |
+--------+--------+-----------+         +--------+---------------------+

+----------------------------------+----------+-------+--------+-------+-------+--------+
| METRIC                           | DRIFT    | DRIFT | HBase  | SPARK | REDIS | HADOOP |
|                                  | (LZ4mem) | (mem) | (mem)  |       |       |        |
+----------------------------------+----------+-------+--------+-------+-------+--------+
| 1. Load Table 1 - Views          | 71.440s  |       | 72.10s |       |       | 0      |
+----------------------------------+----------+-------+--------+-------+-------+--------+
| 2. Load Table 2 - Syncs          | 9.552s   |       | 13.56s |       |       | 0      |
+----------------------------------+----------+-------+--------+-------+-------+--------+
| 3. Memory Used                   |          |       |        |       |       | 0      |
+----------------------------------+----------+-------+--------+-------+-------+--------+
| 4. Memory Occupied               | 222Mb    |       | 1500Mb |       |       | 0      |
+----------------------------------+----------+-------+--------+-------+-------+--------+
| 5. SCAN Syncs for a column value | 0.1s     |       | 0.666s |       |       |        |
+----------------------------------+----------+-------+--------+-------+-------+--------+
| 6. SCAN Views for a url contains | 1.491s   |       | 5.660s |       |       |        |
+----------------------------------+----------+-------+--------+-------+-------+--------+
| 7. COUNT inner join              | 1.659s   |       | 3.060s |       |       |        |
+----------------------------------+----------+-------+--------+-------+-------+--------+
| 8. TRANSFORM inner join          | 22.197s  |       | 0      |       |       |        |
+----------------------------------+----------+-------+--------+-------+-------+--------+
| 9. EXPORT inner join to file     | 33.296s  |       |        |       |       |        |
+----------------------------------+----------+-------+--------+-------+-------+--------+
| 10. RANDOM ACCESS                | N/A      | N/A   |        |       |       | N/A    |
+----------------------------------+----------+-------+--------+-------+-------+--------+

DRIFT - commands to run the benchmark
load views time cat ~/addthis_views_2014-10-31_15.csv.gz | java -jar target/drift-loader.jar --separator '\t' --gzip --keyspace addthis --table views
load syncs: time cat ~/addthis_syncs_2014-10-31_15.csv.gz | java -jar target/drift-loader.jar --separator '\t' --gzip --keyspace addthis --table syncs
scan for a column value in the syncs table: select at_id,vdna_user_uid from syncs where vdna_user_uid= 'ce1e0d6b-6b11-428c-a9f7-c919721c669c'
scan for a url contains a pattern: select at_id,url from views where url contains 'http://www.toysrus.co.uk/Toys-R-Us/Toys/Cars-and-Trains/Cars-and-Playsets'
count the equi inner join between both tables: count (select at_id from syncs join select at_id from views)
export crossed data into local file: time java -jar target/drift-client.jar --keyspace addthis "select vdna_user_uid, timestamp,url FROM ( select vdna_user_uid, at_id from syncs join select at_id,timestamp,url from views)" > ~/vdna-addthis-export.csv
export crossed data into vdna keyspace: time java -jar target/drift-client.jar --keyspace addthis "select vdna_user_uid from syncs join select timestamp,url from views" | java -jar target/drift-loader.jar --separator '\t' --keyspace vdna --table pageviews


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
