package net.imagini.drift.hadoop;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import net.imagini.drift.cluster.DriftManager;
import net.imagini.drift.cluster.DriftManagerZk;
import net.imagini.drift.cluster.DriftNodeLoaderWorker;
import net.imagini.drift.types.DriftSchema;
import net.imagini.drift.types.TypeUtils;
import net.imagini.drift.utils.View;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DriftLoaderReducer takes input for each partition and connects writes them
 * through to the associated drift node and returns the count of loaded records
 */
public class DriftLoaderReducer extends
        Reducer<IntWritable, BytesWritable, IntWritable, IntWritable> {

    static Logger log = LoggerFactory.getLogger(DriftLoaderReducer.class);

    private DriftManager manager;
    private String keyspace;
    private String table;
    private DriftSchema driftSchema;

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        Configuration conf = context.getConfiguration();
        String zkConnect = conf.get("driftZkConnect");
        String clusterId = conf.get("driftCluserId");
        keyspace = conf.get("driftKeyspace");
        table = conf.get("driftTable");
        driftSchema = DriftSchema.fromString(conf.get("driftSchema"));
        log.info("DRIFT ZK CONNECT " + zkConnect);
        log.info("DRIFT CLUSTER ID " + clusterId);
        manager = new DriftManagerZk(zkConnect, clusterId);
    }

    @Override
    protected void reduce(IntWritable nodeId, Iterable<BytesWritable> records,
            Context context) throws IOException, InterruptedException {
        log.info("DRIFT LOADER REDUCER FOR KEYSPACE: " + keyspace);
        log.info("DRIFT LOADER REDUCER FOR TABLE: " + table);
        log.info("DRIFT LOADER REDUCER FOR NODE: " + nodeId.get());
        DriftNodeLoaderWorker worker = new DriftNodeLoaderWorker(nodeId.get(),
                keyspace, table, manager.getNodeConnector(nodeId.get()));
        ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.submit(worker);
        try {
            View[] recordView = new View[driftSchema.size()];
            for (BytesWritable record : records) {
                byte[] recordBytes = record.getBytes();
                int offset = 0;
                for (int i = 0; i < driftSchema.size(); i++) {
                    int len = TypeUtils.sizeOf(driftSchema.get(i), recordBytes,
                            offset);
                    recordView[i] = new View(recordBytes, offset, offset + len
                            - 1, recordBytes.length);
                    offset += len;
                }
                worker.process(recordView);
            }
        } finally {
            worker.finish();
            executor.shutdown();
            executor.awaitTermination(100, TimeUnit.SECONDS);
            long totalLoadedCount =  worker.ackLoadedCount();
            context.write(nodeId, new IntWritable((int) totalLoadedCount));
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        manager.close();
    }
}
