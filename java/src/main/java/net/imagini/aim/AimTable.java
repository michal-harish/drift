package net.imagini.aim;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import joptsimple.internal.Strings;
import net.imagini.aim.node.Segment;
import net.imagini.aim.pipes.Pipe;

/**
 * @author mharis
 */
public class AimTable {

    private String name;
    private Integer segmentSize;
    private LinkedList<AimDataType> def = new LinkedList<>();
    private Map<String,Integer> colIndex = new HashMap<>();
    private LinkedList<AimSegment> segments = new LinkedList<>();
    private AimSegment currentSegment; 
    private AtomicLong originalSize = new AtomicLong(0);
    private AtomicLong size = new AtomicLong(0);
    private AtomicLong count = new AtomicLong(0);

    //TODO segmentSize in bytes rather than records
    public AimTable(String name, Integer segmentSize, LinkedHashMap<String,AimDataType> columnDefs) {
        this.name = name;
        for(Entry<String,AimDataType> columnDef: columnDefs.entrySet()) {
            def.add(columnDef.getValue());
            colIndex.put(columnDef.getKey(), def.size()-1);
        }
        this.segmentSize = segmentSize;
    }

    public AimDataType def(String colName) {
        checkColumn(colName);
        return def.get(colIndex.get(colName));
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
        if (!colIndex.containsKey(colName)) {
            throw new IllegalArgumentException(
                "Column `"+colName+"` is not defined for table `"+name+"`. Available columns are: " 
                + Strings.join(colIndex.keySet().toArray(new String[def.size()-1]), ",")
            );
        }
    }

    public void append(Pipe pipe) throws IOException {
        //TODO thread-safe concurrent atomic appends, e.g. one record as whole, and strictly no more than segmentSize
        boolean hasData = false;
        for(int col = 0; col < def.size() ; col++) {
            AimDataType type = def.get(col); 
            byte[] value = pipe.read(type);
            if (!hasData) {
                hasData = true;
                if (count.get() % segmentSize == 0) {
                    currentSegment = new Segment(def.size());
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

    public InputStream[] range(int startSegment, int endSegment, String... columnNames) throws IOException {
        InputStream[] result = new InputStream[columnNames.length];
        int i = 0; for(String colName: columnNames) {
            result[i++] = new ColumnInputStream(startSegment, endSegment, colIndex.get(colName));
        }
        return result;
    }

    public InputStream range(int startSegment, int endSegment, String column) throws IOException {
        return new ColumnInputStream(startSegment, endSegment, colIndex.get(column));
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
