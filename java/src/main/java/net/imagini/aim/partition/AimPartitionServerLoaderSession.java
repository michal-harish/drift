package net.imagini.aim.partition;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import net.imagini.aim.segment.AimSegment;
import net.imagini.aim.segment.AimSegmentQuickSort;
import net.imagini.aim.segment.AimSegmentUnsorted;
import net.imagini.aim.tools.Pipe;
import net.imagini.aim.types.AimType;
import net.imagini.aim.utils.ByteUtils;
import net.imagini.aim.utils.BlockStorageLZ4;

/**
 * Non-Thread safe, the session is a single-threaded context
 * @author mharis
 *
 */
public class AimPartitionServerLoaderSession extends Thread  {

    /* Zero-copy support
     * FIXME COLUMN_BUFFER_SIZE should be configurable for different schemas
     */
    final public static Integer COLUMN_BUFFER_SIZE = 2048; 

    private Pipe pipe;
    private AimPartition partition;
    private Integer count = 0;
    private ByteBuffer record;
    private AimSegment currentSegment;

    public AimPartitionServerLoaderSession(AimPartition partition, Pipe pipe) throws IOException {
        this.pipe = pipe;
        this.partition = partition;
        String expectSchema = partition.schema.toString();
        String actualSchema = pipe.read();
        if (!actualSchema.equals(expectSchema)) {
            throw new IOException("Invalid loader schema, \nexpecting: " + expectSchema +"\nreceived:  " + actualSchema);
        }
        //TODO assuming fixed column size is not very clever but we need fixed record buffer
        record = ByteUtils.createBuffer(COLUMN_BUFFER_SIZE * partition.schema.size());
    }

    @Override public void run() {
        System.out.println("Loading into " + partition);
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
                        for(AimType type: partition.schema.fields()) {
                            pipe.read(type.getDataType(), record);
                        }
                        record.flip();
                        currentSegment.appendRecord(record);
                        count++;
                        if (currentSegment.getOriginalSize() > partition.segmentSizeBytes) {
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
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            System.out.println("Loading into " + partition + " finished");
            try { pipe.close(); } catch (IOException e) { }
        }
    }

    private void addCurrentSegment() throws IOException {
        try {
            currentSegment.close();
            if (currentSegment.getOriginalSize()>0) {
                partition.add(currentSegment);
                currentSegment = null;
            }
            createNewSegmentIfNull();
        } catch (Throwable e) {
            throw new IOException(e);
        }
    }

    private void createNewSegmentIfNull() throws IOException, InstantiationException, IllegalAccessException {
        if (currentSegment == null) {
            if (partition.keyColumn == null) {
                currentSegment = new AimSegmentUnsorted(partition.schema, BlockStorageLZ4.class);
            } else {
                currentSegment = new AimSegmentQuickSort(partition.schema, partition.keyColumn, partition.sortOrder, BlockStorageLZ4.class);
            }
        }
    }

}
