package net.imagini.aim;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import joptsimple.internal.Strings;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

/**
 * @author mharis
 */
public class AimTable {

    private String name;
    private Integer segmentSize;
    private LinkedHashMap<String,AimDataType> def;
    private LinkedList<AimSegment> segments = new LinkedList<>();
    private LinkedHashMap<String,LZ4BlockOutputStream> lz4 = new LinkedHashMap<>();
    private LinkedHashMap<String,ByteArrayOutputStream> currentSegment = null;
    private AtomicLong currentOriginalSize = new AtomicLong(0);
    private AtomicLong originalSize = new AtomicLong(0);
    private AtomicLong size = new AtomicLong(0);
    private Long count = 0L;

    public AimTable(String name, Integer segmentSize, LinkedHashMap<String,AimDataType> columnDefs) {
        this.name = name;
        this.def = columnDefs;
        this.segmentSize = segmentSize;
    }

    public AimDataType def(String colName) {
        checkColumn(colName);
        return def.get(colName);
    }

    private void checkColumn(String colName) throws IllegalArgumentException {
        if (!def.containsKey(colName)) {
            throw new IllegalArgumentException(
                "Column `"+colName+"` is not defined for table `"+name+"`. Available columns are: " 
                + Strings.join(def.keySet().toArray(new String[def.size()-1]), ",")
            );
        }
    }

    protected void flushSegment() throws IOException {
        for(Entry<String,AimDataType> c: def.entrySet()) {
            lz4.get(c.getKey()).flush(); 
        }
    }

    protected void closeSegment() throws IOException {
        //TODO thread-safe swap
        for(Entry<String,AimDataType> c: def.entrySet()) {
            lz4.get(c.getKey()).close(); 
            size.addAndGet(currentSegment.get(c.getKey()).size());
        }
        originalSize.addAndGet(currentOriginalSize.getAndSet(0));
        segments.add(new AimSegment(currentSegment));
        lz4.clear();
        currentSegment = null;
    }

    public void append(Pipe pipe) throws IOException {
        openSegment();
        for(Entry<String,AimDataType> col: def.entrySet()) {
            LZ4BlockOutputStream lz4 = this.lz4.get(col.getKey());
            AimDataType type = col.getValue(); 
            //System.out.println("loading " + type);
            byte[] value = pipe.read(type);
            //System.out.println("value " + Pipe.convert(type, value));
            if (type.equals(Aim.STRING)) {
                Pipe.write(lz4,value.length);
                currentOriginalSize.addAndGet(4);
            }
            lz4.write(value);
            currentOriginalSize.addAndGet(value.length);
        }
        count ++;
        if (count % segmentSize == 0) {
            closeSegment();
        }
    }

    private void openSegment() {
        //TODO thread-safe 
        if (currentSegment == null) {
            currentSegment = new LinkedHashMap<>();
            for(String col: def.keySet()) {
                ByteArrayOutputStream currentColumnSegment = new ByteArrayOutputStream(65535);
                currentSegment.put(col, currentColumnSegment);
                lz4.put(col, new LZ4BlockOutputStream(
                    currentColumnSegment, 
                    65535,
                    LZ4Factory.fastestInstance().highCompressor(),
                    XXHashFactory.fastestInstance().newStreamingHash32(0x9747b28c).asChecksum(), 
                    true
                ));
            }
        }
    }

    public long getCount() {
        return count;
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

    public InputStream[] range(int startSegment, int endSegment, String... columnNames) throws IOException {
        InputStream[] result = new InputStream[columnNames.length];
        int i = 0; for(String col: columnNames) {
            result[i++] = new ColumnInputStream(startSegment, endSegment, col);
        }
        return result;
    }

    public InputStream range(int startSegment, int endSegment, String column) throws IOException {
        return new ColumnInputStream(startSegment, endSegment, column);
    }

    private class ColumnInputStream extends InputStream {
        private String column;
        protected int segmentIndex = 0;
        private int endSegment = 0;
        private InputStream segmentStream;
        public ColumnInputStream(int startSegment, int endSegment, String column) throws IOException {
            this.segmentIndex = startSegment;
            this.endSegment = Math.min(endSegment, segments.size());
            this.column = column;
            nextSegment();
        }
        protected void nextSegment() throws EOFException {
            if (segmentIndex > endSegment) throw new EOFException();
            byte[] segment = segments.get(segmentIndex++).get(column);
            segmentStream = new LZ4BlockInputStream(new ByteArrayInputStream(segment));
        }
        @Override public final int read() throws IOException {
            int result;
            if (-1 == (result = segmentStream.read())) {
                nextSegment();
                result = segmentStream.read();
            }
            return result;
        }
    }

}
