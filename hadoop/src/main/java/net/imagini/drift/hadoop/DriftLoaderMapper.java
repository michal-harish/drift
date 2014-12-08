package net.imagini.drift.hadoop;

import java.io.IOException;

import net.imagini.drift.types.DriftSchema;
import net.imagini.drift.types.DriftType;
import net.imagini.drift.utils.ByteUtils;
import net.imagini.drift.utils.View;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatBaseInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mapper takes input data and normalises them into drift schema structure and
 * emits it with the partition (IntWritable) of the key value
 */
public class DriftLoaderMapper extends
        Mapper<WritableComparable<?>, HCatRecord, IntWritable, BytesWritable> {

    static Logger log = LoggerFactory.getLogger(DriftLoaderMapper.class);

    private HCatSchema hcatSchema;
    private DriftSchema driftSchema;
    private int[] hCatColumns;
    private int numDriftNodes;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        hcatSchema = HCatBaseInputFormat.getTableSchema(conf);
        log.info("DRIFT SCHEMA: " + conf.get("driftSchema"));
        driftSchema = DriftSchema.fromString(conf.get("driftSchema"));
        String[] mapping = conf.getStrings("mapping");
        hCatColumns = new int[mapping.length];
        for(int c = 0; c < driftSchema.size(); c++) {
            HCatFieldSchema hCatField = hcatSchema.get(mapping[c]);
            hCatColumns[c] = hcatSchema.getPosition(mapping[c]);
            log.info("HCAT-DRIFT SCHEMA MAPPING: " + driftSchema.name(c) + " -> " + hCatField.getName());
        }
        numDriftNodes = conf.getInt("driftNumNodes", -1);
        log.info("DRIFT NUM NODES: " + numDriftNodes);
    }

    @Override
    protected void map(WritableComparable<?> key, HCatRecord value, Context context) throws IOException, InterruptedException {
        //TODO parse the entire hcat row into a drift row and apply filter
        Object partner_id_space = value.get(24);
        if (partner_id_space != null && partner_id_space.toString().equals("at_id")) {
            byte[][] parsed = new byte[driftSchema.size()][];
            int driftRecordLen = 0;
            for(int c = 0; c < driftSchema.size(); c++) {
                int hCatColumn = hCatColumns[c];
                Object val = value.get(hCatColumn);
                String hcatValueAsString = val == null ? null: val.toString();
                DriftType driftType = driftSchema.get(c);
                parsed[c] = driftType.convert(hcatValueAsString);
                driftRecordLen += parsed[c].length;
            }
            byte[] recordBytes = new byte[driftRecordLen];
            int offset = 0;
            for(int c = 0; c < driftSchema.size(); c++) {
                ByteUtils.copy(parsed[c], 0, recordBytes, offset, parsed[c].length);
                offset += parsed[c].length;
            }
            int partition = driftSchema.get(0).partition(new View(parsed[0]), numDriftNodes) + 1;
            context.write(new IntWritable(partition), new BytesWritable(recordBytes));
        }
    }
}
