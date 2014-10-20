package net.imagini.aim.partition;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

import net.imagini.aim.cluster.AimQuery;
import net.imagini.aim.segment.AimFilter;
import net.imagini.aim.tools.Pipe;
import net.imagini.aim.tools.Tokenizer;
import net.imagini.aim.types.AimDataType;
import net.imagini.aim.types.AimSchema;
import net.imagini.aim.types.AimType;

import org.apache.commons.lang3.StringUtils;

public class AimPartitionServerQuerySession extends Thread {

    private Pipe pipe;
    private AimPartition table;

    public AimPartitionServerQuerySession(AimPartition table, Pipe pipe) throws IOException {
        this.pipe = pipe;
        this.table = table;
    }

    @Override
    public void run() {
        try {
            AimSchema schema = table.schema;
            AimQuery query = new AimQuery(table);
            Integer range = null;
            AimFilter filter = null;
            while(true) {
                String command;
                Queue<String> cmd;
                String input = pipe.read();
                try {
                    cmd = Tokenizer.tokenize(input);
                    command = cmd.poll().toUpperCase();
                    switch(command.toUpperCase()) {

                        default: break;

                        case "STATS": handleStats(cmd); break;

                        case "ALL": range = null; filter = null; break;
                        case "LAST": range = table.getNumSegments()-1; filter = null; break;
                        case "RANGE": handleRangeQuery(cmd,query); filter = null; break;
                        case "FILTER": 
                            if (cmd.size()>0) {
                                filter = AimFilter.fromTokenQueue(schema, cmd); //range, query,  
                            }
                            executeCount(range, query, filter);
                            break;

                        case "SELECT": 
                            if (cmd.size()>0) {
                                schema = table.schema.subset(new ArrayList<String>(cmd));
                            }
                            executeSelect(range, query, schema, filter, true); 
                            break;
                    }
                    pipe.write(false);
                    pipe.flush();
                } catch (Exception e) {
                    try {
                        pipe.write(true);
                        pipe.write("ERROR");
                        pipe.write(exceptionAsString(e));
                        pipe.write(false);
                        pipe.flush();
                    } catch (Exception e1) {
                        System.out.println("Could not send error response to the clien: ");
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e) {
            try {
                pipe.close();
            } catch (IOException e1) {}
            return;
        }
    }

    private void handleStats(Queue<String> cmd) throws IOException {
        pipe.write(true);
        pipe.write("STATS");
        pipe.write(table.schema.toString());
        pipe.write(table.getCount());
        pipe.write(table.getNumSegments());
        pipe.write(table.getSize());
        pipe.write(table.getOriginalSize());
    }

    private AimQuery handleRangeQuery(Queue<String> cmd, AimQuery query) {
        // TODO Auto-generated method stub
        return null;
    }

    private void executeCount(
        Integer range, 
        AimQuery query, 
        AimFilter filter
    ) throws IOException, ExecutionException {
        long t = System.currentTimeMillis();
        t = (System.currentTimeMillis()-t);
        query.range(range);
        pipe.write(true);
        pipe.write("COUNT");
        Long count = query.count(filter);
        pipe.write(filter == null ? null : filter.toString());
        pipe.write(count);
        pipe.write((long)table.getCount());
        pipe.flush();
    }

    private void executeSelect(
        Integer range, 
        AimQuery query, 
        AimSchema schema, 
        AimFilter filter,
        boolean fetch
    ) throws IOException {
        query.range(range);
        Pipe result = query.select(filter, schema.names());
        pipe.write(true);
        pipe.write("RESULT");
        pipe.write(schema.toString());
        pipe.write(filter == null ? null : filter.toString());

        long count = 0; 
        try {
            boolean written;
            while(true) {
                written = false;
                for(AimType type: schema.fields()) {
                    AimDataType dataType = type.getDataType();
                    if (!fetch) {
                        result.skip(dataType);
                    } else {
                        byte[] val = result.read(dataType);
                        if (!written) pipe.write(written = true);
                        pipe.write(dataType.getDataType(), val);
                    }

                }
                count++;
            }
        } catch (Exception e) {
            pipe.write(false);
            pipe.write((long)count);
            pipe.write((long)table.getCount());
            pipe.write((e instanceof EOFException ? "" : exceptionAsString(e))); //success flag
            pipe.flush();
        }

    }


    public static String exceptionAsString(Exception e) {
        String[] trace = new String[e.getStackTrace().length];
        int i = 0; for(StackTraceElement t:e.getStackTrace()) trace[i++] = t.toString();
        return e.toString() + "\n"  + StringUtils.join(trace,"\n");
    }

}
