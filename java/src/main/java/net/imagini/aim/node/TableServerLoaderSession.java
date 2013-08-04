package net.imagini.aim.node;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import net.imagini.aim.AimSegment;
import net.imagini.aim.AimType;
import net.imagini.aim.pipes.Pipe;

/**
 * Non-Thread safe, the session is a single-threaded context
 * @author mharis
 *
 */
public class TableServerLoaderSession extends Thread  {

    private Pipe pipe;
    private AimTable table;
    private Integer count = 0;
    private AimSegment currentSegment;
    private ByteBuffer buffer = ByteBuffer.allocate(1000000);

    public TableServerLoaderSession(AimTable table, Pipe pipe) throws IOException {
        this.pipe = pipe;
        this.table = table;
        String expectSchema = table.schema.toString();
        String actualSchema = pipe.read();
        if (!actualSchema.equals(expectSchema)) {
            throw new IOException("Invalid loader schema for table '"+ table.name +"', \nexpecting: " + expectSchema +"\nreceived:  " + actualSchema);
        }
    }

    @Override public void run() {
        System.out.println("Loading into " + table.name);
        try {
            long t = System.currentTimeMillis();
            try {
                while (true) {
                    if (interrupted())  break;
                    if (currentSegment == null) {
                        //TODO add sortColumn to schema and if set instantiate sorted
                        //currentSegment = new Segment(table.schema);
                        currentSegment = new Segment/*Sorted*/(table.schema);//, table.sortColumn, table.sortOrder);
                    }
                    try { 
                        for(AimType type: table.schema.def()) {
                            pipe.read(type.getDataType(), buffer);
                        }
                        commitCurrentSegment(false);
                    } catch (EOFException e) {
                        commitCurrentSegment(true);
                        throw e;
                    }
                }
            } catch (EOFException e) {
                System.out.println("load(EOF) records: " + count
                        + " time(ms): " + (System.currentTimeMillis() - t)
                );
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            System.out.println("Loading into " + table.name + " finished");
            try { pipe.close(); } catch (IOException e) { }
        }
    }

    private void commitCurrentSegment(boolean force) throws IOException {
        if (currentSegment != null) {
            boolean commit = force || currentSegment.getSize() > table.segmentSizeBytes;
            if (commit || buffer.position() > 65535) {
                buffer.flip();
                currentSegment.append(buffer);
                buffer.clear();
            }
            if (commit) {
                try {
                    currentSegment.close();
                    if (currentSegment.getOriginalSize() > 0) {
                        table.add(currentSegment);
                    }
                    currentSegment = null;
                } catch (IllegalAccessException e) {
                    throw new IOException(e);
                }
            }
        }
    }
    

}
