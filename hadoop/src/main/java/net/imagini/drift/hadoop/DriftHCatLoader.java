package net.imagini.drift.hadoop;

import net.imagini.drift.cluster.DriftManager;
import net.imagini.drift.cluster.DriftManagerZk;
import net.imagini.drift.types.DriftSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

public class DriftHCatLoader extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new DriftHCatLoader(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        String zkConnect = "bl-mharis-d02";
        String clusterId = "benchmark4B";
        String keyspace = "addthis";
        String table = "syncs";
        Configuration conf = getConf();
        String hcatTable = "hcat_events_rc";
        String filter = "topic=\"datasync\" and d=\"2014-10-31\"";
        conf.set("driftZkConnect", zkConnect);
        conf.set("driftCluserId", clusterId);
        conf.set("driftKeyspace", keyspace);
        conf.set("driftTable", table);
        //TODO fetch schema and number of nodes from the manager
        DriftManager manager = new DriftManagerZk(zkConnect, clusterId);
        int numNodes = manager.getNodeConnectors().size();
        DriftSchema schema = manager.getDescriptor(keyspace, table).schema;
        System.out.println("DRIFT CLUSTER ZK CONNECT: " + zkConnect);
        System.out.println("DRIFT CLUSTER ID: " + clusterId);
        System.out.println("DRIFT CLUSTER NODES: " + numNodes);
        System.out.println("DRIFT KEYSPACE: " + keyspace);
        System.out.println("DRIFT TABLE: " + table);
        System.out.println("DRIFT SCHEMA: " + schema.toString());
        conf.set("driftSchema", schema.toString());
        conf.set("driftNumNodes", String.valueOf(numNodes));
        //TODO provide generic way of mapping + conf.set("filter", "partner_id_space=\"at_id\"");
        conf.setStrings("mapping", "partner_user_id", "useruid", "timestamp");

        JobConf jobConf = new JobConf(conf);
        Job job = Job.getInstance(jobConf, "drift.loader-" + filter);
        job.setJarByClass(DriftHCatLoader.class);

        job.setInputFormatClass(HCatInputFormat.class);
        job.setMapperClass(DriftHCatMapper.class);
        HCatInputFormat.setInput(job, null, hcatTable, filter);
        job.setReducerClass(DriftLoaderReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setNumReduceTasks(numNodes);
        TextOutputFormat.setOutputPath(job, new Path("/user/mharis/drift-load"));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : -1;
    }
}
