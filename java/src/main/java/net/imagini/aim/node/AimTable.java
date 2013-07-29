package net.imagini.aim.node;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import net.imagini.aim.Aim;
import net.imagini.aim.Aim.SortOrder;
import net.imagini.aim.AimFilter;
import net.imagini.aim.AimSchema;
import net.imagini.aim.AimSegment;
import net.imagini.aim.AimType;
import net.imagini.aim.AimTypeAbstract.AimDataType;
import net.imagini.aim.AimUtils;
import net.imagini.aim.ByteArrayWrapper;
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
     * ZeroCopy 
     * Open multiple segments - this is where streaming-merge-sort happens
     * Note: merge sort is combined with TreeMap O=log(n) indexing so
     * that for large number of segments it still performs well.
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
            final AimFilter filter, 
            final String... columnNames
        ) throws IOException {
            //TODO sortColumn columns must be present, so will be added if missing
            final int[] seg = new int[endSegment-startSegment+1];
            for(int i=startSegment; i<= endSegment; i++) seg[i-startSegment] = i;
            final InputStream[] str = new InputStream[segments.size()];
            final AimSchema subSchema = schema.subset(columnNames);
            for(int s = 0; s <seg.length; s++) {
                str[s] = segments.get(seg[s]).open(filter, Arrays.asList(columnNames));
            }

            final int sortSubColumn = subSchema.get(schema.name( sortColumn));
            return new Pipe() {
                private int currentSegment = -1;
                private int currentColumn = columnNames.length-1;
                private TreeMap<ByteArrayWrapper,Integer> sortIndex = new TreeMap<>();
                final private Boolean[] hasData = new Boolean[segments.size()];
                final private byte[][][] buffer = new byte[segments.size()][schema.size()][Aim.COLUMN_BUFFER_SIZE];
                @Override public byte[] read(AimDataType type) throws IOException {
                    if (++currentColumn == columnNames.length) {
                        currentColumn = 0;
                        readNextRecord();
                    }
                    return buffer[currentSegment][currentColumn];
                }
                private void readNextRecord() throws IOException {
                    if (currentSegment ==-1 ) {
                        for(int s = 0; s <seg.length; s++) {
                            loadRecordFromSegment(s);
                        }
                    }
                    if (currentSegment >=0) {
                        if (hasData[currentSegment]) loadRecordFromSegment(currentSegment);
                    }
                    if (sortIndex.size() == 0) {
                        throw new EOFException();
                    }
                    Entry<ByteArrayWrapper,Integer> next = sortIndex.pollFirstEntry();
                    currentSegment = next.getValue();
                }
                private void loadRecordFromSegment(int s) throws IOException {
                    try {
                        for(String colName: columnNames) {
                            int c = subSchema.get(colName);
                            AimType type = subSchema.get(c);
                            AimUtils.read(str[s],type.getDataType(), buffer[s][c]);
                        }
                        sortIndex.put(new ByteArrayWrapper(buffer[s][sortSubColumn],s), s);
                        hasData[s] = true;
                    } catch (EOFException e) {
                        hasData[s] = false;
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
