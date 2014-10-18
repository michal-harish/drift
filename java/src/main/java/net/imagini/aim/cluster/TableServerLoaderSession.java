package net.imagini.aim.cluster;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import net.imagini.aim.AimPartition;
import net.imagini.aim.AimSegment;
import net.imagini.aim.Pipe;
import net.imagini.aim.AimSegmentLZ4;
import net.imagini.aim.AimSegmentLZ4QuickSort;
import net.imagini.aim.types.AimType;
import net.imagini.aim.utils.AimUtils;

/**
 * Non-Thread safe, the session is a single-threaded context
 * @author mharis
 *
 */
public class TableServerLoaderSession extends Thread  {

    /* Zero-copy support
     * FIXME COLUMN_BUFFER_SIZE should be configurable for different schemas
     */
    final public static Integer COLUMN_BUFFER_SIZE = 2048; 

    private Pipe pipe;
    private AimPartition table;
    private Integer count = 0;
    private ByteBuffer record;
    private AimSegment currentSegment;

    public TableServerLoaderSession(AimPartition table, Pipe pipe) throws IOException {
        this.pipe = pipe;
        this.table = table;
        String expectSchema = table.schema.toString();
        String actualSchema = pipe.read();
        if (!actualSchema.equals(expectSchema)) {
            throw new IOException("Invalid loader schema, \nexpecting: " + expectSchema +"\nreceived:  " + actualSchema);
        }
        //TODO assuming fixed column size is not very clever but we need fixed record buffer
        record = AimUtils.createBuffer(COLUMN_BUFFER_SIZE * table.schema.size());
    }

    @Override public void run() {
        System.out.println("Loading into " + table);
        try {
            long t = System.currentTimeMillis();
            try {
                createNewSegmentIfNull();
                while (true) {
                    if (interrupted())  break;
                    try {
//                        currentSegment.append(pipe.getInputStream());
                        //TODO read full record and check if it will fit within the buffer
                        record.clear();
                        for(AimType type: table.schema.fields()) {
                            pipe.read(type.getDataType(), record);
                        }
                        record.flip();
                        currentSegment.append(record);
                        count++;
                        if (currentSegment.getOriginalSize() > table.segmentSizeBytes) {
                            addCurrentSegment();
                        }
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
            System.out.println("Loading into " + table + " finished");
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
            if (table.keyColumn == null) {
                currentSegment = new AimSegmentLZ4(table.schema);
            } else {
                currentSegment = new AimSegmentLZ4QuickSort(table.schema, table.keyColumn, table.sortOrder);
            }
        }
    }

}
