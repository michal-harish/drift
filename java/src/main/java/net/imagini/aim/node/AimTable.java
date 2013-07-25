package net.imagini.aim.node;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

import net.imagini.aim.AimSchema;
import net.imagini.aim.AimSegment;
import net.imagini.aim.AimType;
import net.imagini.aim.pipes.Pipe;

/**
 * @author mharis
 */
public class AimTable {

    public final AimSchema schema;
    public final String name;
    public final Integer segmentSize;
    private LinkedList<AimSegment> segments = new LinkedList<>();
    private AimSegment currentSegment; 
    private AtomicLong originalSize = new AtomicLong(0);
    private AtomicLong size = new AtomicLong(0);
    private TableServer server;

    //TODO segmentSize in bytes rather than records
    public AimTable(String name, Integer segmentSize, AimSchema schema) throws IOException {
        this.name = name;
        this.schema = schema;
        this.segmentSize = segmentSize;
        System.out.println("Table " + name + " (" + schema.serialize() + ")");
        server = new TableServer(this, 4000);
        server.start();
    }

    public void close() throws IOException {
        try {
            server.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        closeSegment();
    }

    public AimType def(String colName) {
        checkColumn(colName);
        return schema.def(colName);
    }

    public int getNumSegments() {
        return segments.size();
    }

    public long getCount() {
        Long result = 0L;
        for(AimSegment segment: segments) {
            result += segment.getCount();
        }
        return result;
    }
    public long getSize() {
        return size.get();
    }

    public long getOriginalSize() {
        return originalSize.get();
    }

    private void checkColumn(String colName) throws IllegalArgumentException {
        if (!schema.has(colName)) {
            throw new IllegalArgumentException(
                "Column `"+colName+"` is not defined for table `"+name+"`. Available columns are: " 
                + schema.describe()
            );
        }
    }

    //TODO thread-safe concurrent atomic appends instead of synchronization, 
    //e.g. one record as whole, and strictly no more than segmentSize
    public synchronized void append(Pipe pipe) throws IOException {
        if (currentSegment == null || currentSegment.getCount() % segmentSize == 0) {
            currentSegment = new Segment(schema);
        }
        try {
            currentSegment.append(pipe);
            if (currentSegment.getCount() % segmentSize == 0) {
                closeSegment();
            }
        } catch (EOFException e) {
            closeSegment();
            throw e;
        }
    }

    private void closeSegment() throws IOException {
        //TODO thread-safe swap
        if (currentSegment != null) {
            try {
                currentSegment.close();
                if (currentSegment.getOriginalSize() > 0) {
                    size.addAndGet(currentSegment.getSize());
                    originalSize.addAndGet(currentSegment.getOriginalSize());
                    segments.add(currentSegment);
                }
                currentSegment = null;
            } catch (IllegalAccessException e) {
                throw new IOException(e);
            }
        }
    }
    public AimSegment open(int segmentId) throws IOException {
        return segments.get(segmentId);
    }

    public InputStream[] open(int startSegment, int endSegment, String... columnNames) throws IOException {
        InputStream[] result = new InputStream[columnNames.length];
        int i = 0; for(String colName: columnNames) {
            result[i++] = new ColumnInputStream(startSegment, endSegment, schema.get(colName));
        }
        return result;
    }

    private class ColumnInputStream extends InputStream {
        private int column;
        protected int segmentIndex = 0;
        private int endSegment = 0;
        private InputStream columnSegmentInputStream;
        public ColumnInputStream(int startSegment, int endSegment, int column) throws IOException {
            this.segmentIndex = startSegment;
            this.endSegment = Math.min(endSegment, segments.size());
            this.column = column;
            nextSegment();
        }
        protected void nextSegment() throws IOException {
            if (segmentIndex > endSegment) throw new EOFException();
            columnSegmentInputStream = segments.get(segmentIndex++).open(column);
        }
        @Override public final int read() throws IOException {
            int result;
            if (-1 == (result = columnSegmentInputStream.read())) {
                nextSegment();
                result = columnSegmentInputStream.read();
            }
            return result;
        }
    }

}
