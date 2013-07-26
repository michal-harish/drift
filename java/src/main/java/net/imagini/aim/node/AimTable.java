package net.imagini.aim.node;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import net.imagini.aim.AimSchema;
import net.imagini.aim.AimSegment;
import net.imagini.aim.AimType;

/**
 * @author mharis
 */
public class AimTable {

    public final AimSchema schema;
    public final String name;
    public final Integer segmentSize;
    private LinkedList<AimSegment> segments = new LinkedList<>();
    private AtomicInteger numSegments = new AtomicInteger(0);
    private AtomicLong originalSize = new AtomicLong(0);
    private AtomicLong count = new AtomicLong(0);
    private AtomicLong size = new AtomicLong(0);

    //TODO segmentSize in bytes rather than records
    public AimTable(String name, Integer segmentSize, AimSchema schema) throws IOException {
        this.name = name;
        this.schema = schema;
        this.segmentSize = segmentSize;
        System.out.println("Table " + name + " (" + schema.toString() + ")");
    }

    public void add(AimSegment segment) {
        //TODO add is not thread-safe
        segments.add(segment);

        numSegments.incrementAndGet();
        count.addAndGet(segment.getCount());
        size.addAndGet(segment.getSize());
        originalSize.addAndGet(segment.getOriginalSize());
    }

    public AimType def(String colName) {
        checkColumn(colName);
        return schema.def(colName);
    }

    public int getNumSegments() {
        return numSegments.get();
    }

    public long getCount() {
        return count.get();
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

    public AimSegment open(int segmentId) throws IOException {
        return segments.get(segmentId);
    }

    public InputStream[] open(int startSegment, int endSegment, String... columnNames) throws IOException {
        if (columnNames.length == 0) columnNames = schema.getNames();
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
