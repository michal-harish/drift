package net.imagini.drift.hadoop;

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
        Configuration conf = getConf();
        String hcatTable = "hcat_events_rc";
        String filter = "topic=\"datasync\" and d=\"2014-10-31\"";
        conf.set("driftZkConnect", "bl-mharis-d02");
        conf.set("driftCluserId", "benchmark4B");
        conf.set("driftKeyspace", "addthis");
        conf.set("driftTable", "syncs");
        //TODO fetch schema and number of nodes from the manager
        conf.set("driftSchema", "at_id(STRING), vdna_user_uid(UUID), timestamp(LONG)");
        conf.set("driftNumNodes", "32");
        //TODO provide generic way of mapping and filtering
        conf.setStrings("mapping", "partner_user_id", "useruid", "timestamp");
        // TODO conf.set("filter", "partner_id_space=\"at_id\"");
        //
        JobConf jobConf = new JobConf(conf);
        Job job = Job.getInstance(jobConf, "drift.loader-" + filter);
        job.setJarByClass(DriftHCatLoader.class);

        job.setInputFormatClass(HCatInputFormat.class);
        job.setMapperClass(DriftLoaderMapper.class);
        HCatInputFormat.setInput(job, null, hcatTable, filter);
        job.setReducerClass(DriftLoaderReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BytesWritable.class);
        TextOutputFormat.setOutputPath(job, new Path("/user/mharis/drift-load"));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : -1;
    }
}
