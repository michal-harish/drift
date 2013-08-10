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
    private ByteBuffer recordBuffer;
    private ByteBuffer currentSegmentBlock = ByteBuffer.allocate(Aim.LZ4_BLOCK_SIZE);
    private AimSegment currentSegment;

    public TableServerLoaderSession(AimTable table, Pipe pipe) throws IOException {
        this.pipe = pipe;
        this.table = table;
        String expectSchema = table.schema.toString();
        String actualSchema = pipe.read();
        if (!actualSchema.equals(expectSchema)) {
            throw new IOException("Invalid loader schema for table '"+ table.name +"', \nexpecting: " + expectSchema +"\nreceived:  " + actualSchema);
        }
        recordBuffer = ByteBuffer.allocate(Aim.COLUMN_BUFFER_SIZE * table.schema.size());
    }

    @Override public void run() {
        System.out.println("Loading into " + table.name);
        try {
            long t = System.currentTimeMillis();
            try {
                while (true) {
                    if (interrupted())  break;
                    try {
                        //TODO read full record and check if it will fit withing the buffer
                        recordBuffer.clear();
                        for(AimType type: table.schema.def()) {
                            pipe.read(type.getDataType(), recordBuffer);
                        }
                        recordBuffer.flip();
                        if (currentSegmentBlock.position() + recordBuffer.limit() > Aim.LZ4_BLOCK_SIZE ) {
                            compressCurrentBuffer(false);
                        }
                        currentSegmentBlock.put(recordBuffer);
                    } catch (EOFException e) {
                        compressCurrentBuffer(true);
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

    private void compressCurrentBuffer(boolean force) throws IOException {
        currentSegmentBlock.flip();
        if (currentSegmentBlock.limit() > 0) {
            createNewSegmentIfNull();
            if (force || currentSegment.getOriginalSize() + currentSegmentBlock.limit() > table.segmentSizeBytes) {
                addCurrentSegment();
            }
            currentSegment.append(currentSegmentBlock);
            if (force) {
                addCurrentSegment();
            }
            currentSegmentBlock.clear();
        }
    }

    private void addCurrentSegment() throws IOException {
        try {
            currentSegment.close();
            table.add(currentSegment);
            currentSegment = null;
            createNewSegmentIfNull();
        } catch (IllegalAccessException e) {
            throw new IOException(e);
        }
    }

    private void createNewSegmentIfNull() throws IOException {
        if (currentSegment == null) {
            //TODO add sortColumn to schema and if set instantiate sorted
            //currentSegment = new Segment(table.schema);
            currentSegment = new SegmentSorted(table.schema, table.sortColumn, table.sortOrder);
        }
    }

}
