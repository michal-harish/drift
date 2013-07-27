package net.imagini.aim.node;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import net.imagini.aim.Aim.SortOrder;
import net.imagini.aim.AimFilter;
import net.imagini.aim.AimSchema;
import net.imagini.aim.AimSegment;
import net.imagini.aim.AimType;
import net.imagini.aim.AimTypeAbstract.AimDataType;
import net.imagini.aim.pipes.Pipe;

/**
 * @author mharis
 */
public class AimTable {

    public final AimSchema schema;
    public final String name;
    public final Integer segmentSize;
    public final int sortColumn;
    public final SortOrder sortOrder;
    private LinkedList<AimSegment> segments = new LinkedList<>();
    private AtomicInteger numSegments = new AtomicInteger(0);
    private AtomicLong originalSize = new AtomicLong(0);
    private AtomicLong count = new AtomicLong(0);
    private AtomicLong size = new AtomicLong(0);

    //TODO segmentSize in bytes rather than records
    public AimTable(String name, Integer segmentSize, AimSchema schema, String sortField, SortOrder order) throws IOException {
        this.name = name;
        this.sortColumn = schema.get(sortField);
        this.sortOrder = order;
        this.schema = schema;
        this.segmentSize = segmentSize;
        System.out.println("Table " + name + " (" + schema.toString() + ")");
    }

    public int getSortColumn() {
        return sortColumn;
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

    /**
     * Open single segment.
     * @param segmentId
     * @return
     * @throws IOException
     */
    public AimSegment open(int segmentId) throws IOException {
        return segmentId >=0 && segmentId < segments.size() ? segments.get(segmentId) : null;
    }

    /**
     * Open multiple segments - this is where streaming-merge-sort happens.
     * 
     * @param startSegment
     * @param endSegment
     * @param columnNames
     * @return
     * @throws IOException
     */
    public Pipe open(
        int startSegment, 
        int endSegment,
        AimFilter filter, 
        final String... columnNames
    ) throws IOException {
        //TODO sortColumn columns must be present, so will be added if missing
        final int[] seg = new int[endSegment-startSegment+1];
        for(int i=startSegment; i<= endSegment; i++) seg[i-startSegment] = i;
        final Pipe[] str = new Pipe[segments.size()];
        final AimSchema subSchema = schema.subset(columnNames);
        for(int s = 0; s <seg.length; s++) {
            str[s] = segments.get(seg[s]).open(filter, Arrays.asList(columnNames));
        }

        final int sortSubColumn = subSchema.get(schema.name( sortColumn));
        return new Pipe() {
            private int colIndex = columnNames.length-1;
            private AimRecord record;
            private AimRecord[] records = null;
            @Override public byte[] read(AimDataType type) throws IOException {
                if (++colIndex == columnNames.length) {
                    colIndex = 0;
                    record = getNextRecord();
                }
                if (record == null) {
                    throw new EOFException();
                }
                return record.data[colIndex];
            }
            private AimRecord getNextRecord() throws IOException {
                if (records == null) {
                    records = new AimRecord[seg.length];
                    for(int s = 0; s <seg.length; s++) preloadRecord(s);
                }
                //cross-section compare the records, extract winer and remember which segment it came from
                AimRecord winner = null;
                Integer s = null;
                for(int i=0; i<seg.length; i++) if (records[i] !=null) {
                    if (winner == null || (winner.compareTo(records[i]) == sortOrder.getComparator())) {
                        winner = records[i];
                        s = i;
                    }
                }
                if (winner == null) {
                    throw new EOFException();
                } else {
                    //..and replace it from another in the same segment 
                    preloadRecord(s);
                    return winner;
                }
            }
            private void preloadRecord(int s) throws IOException {
                byte[][] data = new byte[columnNames.length][];
                try {
                    for(String colName: columnNames) {
                        int c = subSchema.get(colName);
                        AimType type = subSchema.get(c);
                        data[c] = str[s].read(type.getDataType());
                    }
                    records[s] = new AimRecord(subSchema, data, sortSubColumn);
                } catch (EOFException e) {
                    records[s] = null;
                }
            }
        };
    }

/* 
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
*/
}
