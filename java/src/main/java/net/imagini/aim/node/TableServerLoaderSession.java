package net.imagini.aim.node;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import net.imagini.aim.Aim;
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
    private ByteBuffer record;
    private AimSegment currentSegment;

    public TableServerLoaderSession(AimTable table, Pipe pipe) throws IOException {
        this.pipe = pipe;
        this.table = table;
        String expectSchema = table.schema.toString();
        String actualSchema = pipe.read();
        if (!actualSchema.equals(expectSchema)) {
            throw new IOException("Invalid loader schema for table '"+ table.name +"', \nexpecting: " + expectSchema +"\nreceived:  " + actualSchema);
        }
        record = ByteBuffer.allocate(Aim.COLUMN_BUFFER_SIZE * table.schema.size());
        record.order(Aim.endian);
    }

    @Override public void run() {
        System.out.println("Loading into " + table.name);
        try {
            long t = System.currentTimeMillis();
            try {
                createNewSegmentIfNull();
                while (true) {
                    if (interrupted())  break;
                    try {
                        //TODO read full record and check if it will fit withing the buffer
                        record.clear();
                        for(AimType type: table.schema.def()) {
                            pipe.read(type.getDataType(), record);
                        }
                        record.flip();
                        if (currentSegment.getOriginalSize() + record.limit() > table.segmentSizeBytes) {
                            addCurrentSegment();
                        }
                        currentSegment.append(record);
                    } catch (EOFException e) {
                        addCurrentSegment();
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

    private void addCurrentSegment() throws IOException {
        try {
            currentSegment.close();
            if (currentSegment.getOriginalSize()>0) {
                table.add(currentSegment);
                currentSegment = null;
            }
            createNewSegmentIfNull();
        } catch (IllegalAccessException e) {
            throw new IOException(e);
        }
    }

    private void createNewSegmentIfNull() throws IOException {
        if (currentSegment == null) {
            if (table.sortColumn == null) {
                currentSegment = new Segment(table.schema);
            } else {
                currentSegment = new SegmentSorted(table.schema, table.sortColumn, table.sortOrder);
            }
        }
    }

}
