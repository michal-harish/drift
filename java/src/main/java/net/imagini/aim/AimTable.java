package net.imagini.aim;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

import net.imagini.aim.node.Segment;
import net.imagini.aim.pipes.Pipe;

/**
 * @author mharis
 */
public class AimTable {

    private String name;
    private Integer segmentSize;
    private AimSchema schema;
    private LinkedList<AimSegment> segments = new LinkedList<>();
    private AimSegment currentSegment; 
    private AtomicLong originalSize = new AtomicLong(0);
    private AtomicLong size = new AtomicLong(0);
    private AtomicLong count = new AtomicLong(0);

    //TODO segmentSize in bytes rather than records
    public AimTable(String name, Integer segmentSize, AimSchema schema) {
        this.name = name;
        this.schema = schema;
        this.segmentSize = segmentSize;
    }

    public AimDataType def(String colName) {
        checkColumn(colName);
        return schema.def(colName);
    }

    public String getName() {
        return name;
    }

    public long getCount() {
        return count.get();
    }

    public int getNumSegments() {
        return segments.size();
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

    public void append(Pipe pipe) throws IOException {
        //TODO thread-safe concurrent atomic appends, e.g. one record as whole, and strictly no more than segmentSize
        boolean hasData = false;
        for(int col = 0; col < schema.size() ; col++) {
            AimDataType type = schema.def(col); 
            byte[] value = pipe.read(type);
            if (!hasData) {
                hasData = true;
                if (count.get() % segmentSize == 0) {
                    currentSegment = new Segment(schema);
                }
            }
            currentSegment.write(col, type,value);
            //System.out.println("loading " + type);
            //System.out.println("value " + Pipe.convert(type, value));
        }
        if (count.incrementAndGet() % segmentSize == 0) {
            close();
        }
    }

    public void close() throws IOException {
        //TODO thread-safe swap
        if (currentSegment != null) {
            try {
                currentSegment.close();
                size.addAndGet(currentSegment.getSize());
                originalSize.addAndGet(currentSegment.getOriginalSize());
                segments.add(currentSegment);
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
