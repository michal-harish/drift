1. Benchmark 
==============================================================================================================================
This is a benchmark for Drift and HBase solving the same use-case with a single instance and only in-memory resources: 
we took 2 tables containing same time frame of pageviews(cca 6m) and syncs (cca 1m) and compared load, memory usage and scan query times:

DRIFT - LZ4 in-memory sequential compression
71.440s: load pageviews time cat ~/addthis_pageviews_2014-10-31_15.csv.gz | java -jar target/drift-loader.jar --separator '\t' --gzip --keyspace addthis --table pageviews
9.552s: load syncs: time cat ~/addthis_syncs_2014-10-31_15.csv.gz | java -jar target/drift-loader.jar --separator '\t' --gzip --keyspace addthis --table syncs
222Mb: memory used with LZ4 compression
0.99s: scan for a column value in the syncs table: select at_id,vdna_user_uid from syncs where vdna_user_uid= 'ce1e0d6b-6b11-428c-a9f7-c919721c669c'
1.491s: scan for a url contains a pattern: select at_id,url from pageviews where url contains 'http://www.toysrus.co.uk/Toys-R-Us/Toys/Cars-and-Trains/Cars-and-Playsets'
1.659s: count the equi inner join between both tables: count (select at_id from syncs join select at_id from pageviews)
23.197s: export crossed data into local file: time java -jar target/drift-client.jar --keyspace addthis "select vdna_user_uid, timestamp,url FROM ( select vdna_user_uid, at_id from syncs join select at_id,timestamp,url from pageviews)" > ~/vdna-addthis-export.csv
33.296s: export crossed data into vdna keyspace: time java -jar target/drift-client.jar --keyspace addthis "select vdna_user_uid from syncs join select timestamp,url from pageviews" | java -jar target/drift-loader.jar --separator '\t' --keyspace vdna --table pageviews


HBASE - wihtout in-memory compression 
150s: load pageviews 
20s: load syncs: 
1500Mb: memory used
....: scan for a column value in the syncs table: ...vdna_user_uid= 'ce1e0d6b-6b11-428c-a9f7-c919721c669c'
9.0s: scan for a url contains patter in the pageviews table: ... url contains 'http://www.toysrus.co.uk/Toys-R-Us/Toys/Cars-and-Trains/Cars-and-Playsets'
5.0s:  count the equi inner join between both tables: ...
....: export crossed data into local file: 
....: export crossed data into vdna keyspace (select vdna_user_uid from syncs join select timestamp,url from pageviews)


#hive add this pageviews  2014-10-31 15:00-16:00 (6 million records) (0.63Gb uncompressed) (0.22Gb compressed)
bl-yarnpoc-p01 hive> select count(1) from hcat_addthis_raw_view_gb where d='2014-10-31' and timestamp>=1414767600000 and timestamp<1414771200000;
bl-yarnpoc-p01 ~> hive -e "select uid, url, timestamp from hcat_addthis_raw_view_gb where d='2014-10-31' and timestamp>=1414767600000 and timestamp<1414771200000;" > addthis_pageviews_2014-10-31_15.csv
bl-yarnpoc-p01 ~> gzip addthis_pageviews_2014-10-31_15.csv
scp mharis@bl-yarnpoc-p01:~/addthis_pageviews_2014-10-31_15.csv.gz ~/

#hive add this syncs  2014-10-31 15:00-16:00
bl-yarnpoc-p01 hive> select count(1) from hcat_events_rc where d='2014-10-31' and partner_id_space='at_id' and topic='datasync' and timestamp>=1414767600 and timestamp<1414771200;
bl-yarnpoc-p01 ~> hive -e "select partner_user_id, useruid, concat(timestamp,'000') from hcat_events_rc where d='2014-10-31' and partner_id_space='at_id' and topic='datasync' and timestamp>=1414767600 and timestamp<1414771200" > addthis_syncs_2014-10-31_15.csv
bl-yarnpoc-p01 ~> gzip addthis_syncs_2014-10-31_15.csv
scp mharis@bl-yarnpoc-p01:~/addthis_syncs_2014-10-31_15.csv.gz ~/
