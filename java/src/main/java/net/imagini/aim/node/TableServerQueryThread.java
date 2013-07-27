package net.imagini.aim.node;

import java.io.EOFException;
import java.io.IOException;

import net.imagini.aim.AimFilterSet;
import net.imagini.aim.AimQuery;
import net.imagini.aim.AimSchema;
import net.imagini.aim.pipes.Pipe;

public class TableServerQueryThread extends Thread {

    private Pipe pipe;
    private AimTable table;

    public TableServerQueryThread(AimTable table, Pipe pipe) throws IOException {
        this.pipe = pipe;
        this.table = table;
    }

    @Override
    public void run() {
        try {
            while(true) {
                String cmd = pipe.read();
                AimSchema schema;
                AimFilterSet set;
                AimQuery query;
                Pipe result;
                switch(cmd) {
                    case "STATS":
                        pipe.write(table.name);
                        pipe.write(table.schema.toString());
                        pipe.write(table.getCount());
                        pipe.write(table.getNumSegments());
                        pipe.write(table.getSize());
                        pipe.write(table.getOriginalSize());
                        pipe.flush();
                        break;
                    case "FILTER":
                        query = new AimQuery(table);
                        long t = System.currentTimeMillis();

                        set = query.filter("user_quizzed").in("true")
                            .and("timestamp").greaterThan("20")
                            .and("api_key").contains("test")
                            .and("post_code").not().in("EC2 A1","EC2 A11","EC2 A21")
                            .go();
                        t = (System.currentTimeMillis()-t);

                        set.write(pipe);
                        pipe.flush();
                        //select
                        //result = query.select(set,cols);
                        break;
                    case "LAST":
                        query = new AimQuery(table, table.getNumSegments()-1);
                        schema = table.schema.subset("user_uid","user_quizzed","timestamp","post_code");
                        result = query.select(schema.names());
                        copy(schema,result,pipe);
                        break;
                    case "ALL":
                        query = new AimQuery(table);
                        schema = table.schema.subset("user_uid","user_quizzed","timestamp","post_code");
                        result = query.select(schema.names());
                        copy(schema,result,pipe);
                        break;
                    case "TEST":
                        t = System.currentTimeMillis();
                        query = new AimQuery(table);
                        schema = table.schema.subset("timestamp","user_uid","user_quizzed","api_key","post_code","url");
                        result = query.select(
                            query.filter("user_quizzed").in("true").and("api_key").equals("debenhams"),
                            schema.names()
                        );
                        t = (System.currentTimeMillis()-t);
                        copy(schema,result,pipe);
                        System.out.println("Filetered & sorted select: " + t +" ms");
                        break;
                    default: pipe.write("Unknown command " + cmd);
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void copy(AimSchema schema, Pipe result, Pipe pipe) throws IOException {
        pipe.write(schema.toString());
        try {
            while(true) {
                AimRecord record = new AimRecord(schema, result, table.sortColumn);
                pipe.write(true);
                pipe.write(record.getBytes());
            }
        } catch (EOFException e) {
            pipe.write(false);
            pipe.flush();
        }
    }
}
