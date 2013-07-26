package net.imagini.aim.node;

import java.io.EOFException;
import java.io.IOException;

import net.imagini.aim.AimFilterSet;
import net.imagini.aim.AimQuery;
import net.imagini.aim.AimType;
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
                AimQuery query;
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

                        AimFilterSet set = query.filter("user_quizzed").in("true")
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
                        Pipe result = query.select();
                        copy(result,pipe);
                        break;
                    default: pipe.write("Unknown command " + cmd);
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void copy(Pipe result, Pipe pipe) throws IOException {
        pipe.write(table.schema.toString());
        try {
            while(true) {
                int i = 0 ; for(AimType type: table.schema.def()) {
                    byte[] value = result.read(type.getDataType());
                    if (i++==0) pipe.write((byte)1);
                    pipe.write(type.getDataType(), value);
                }
            }
        } catch (EOFException e) {
            pipe.write((byte)0);
            pipe.flush();
        }
    }
}
