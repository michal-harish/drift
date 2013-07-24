package net.imagini.aim.node;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import net.imagini.aim.Aim;
import net.imagini.aim.AimDataType;
import net.imagini.aim.AimFilter;
import net.imagini.aim.AimSchema;
import net.imagini.aim.AimSegment;
import net.imagini.aim.pipes.Pipe;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

import org.apache.commons.io.output.ByteArrayOutputStream;

/**
 * zero-copy open methods, i.e. multiple stream readers should be able to operate without 
 * doubling the memory foot-print.
 * @author mharis
 */
public class Segment implements AimSegment {

    private boolean writable;
    private AimSchema schema;
    private LinkedHashMap<Integer,byte[]> columnar = new LinkedHashMap<>();
    private LinkedHashMap<Integer,ByteArrayOutputStream> buffers = null;
    private LinkedHashMap<Integer,LZ4BlockOutputStream> lz4 = null;
    private AtomicLong size = new AtomicLong(0);
    private AtomicLong originalSize = new AtomicLong(0);
    //private AtomicLong originalSize = new AtomicLong(0);

    //TODO public AimSegment(..mmap file)

    /**
     * Read-only Segment
     * @param data
     */
    public Segment(AimSchema schema,LinkedList<ByteArrayOutputStream> data) {
        this.schema = schema;
        this.writable = false;
        int i = 0; for(ByteArrayOutputStream b: data) {
            columnar.put(i++, b.toByteArray());
        }
    }

    /**
     * Append-only segment
     */
    public Segment(AimSchema schema) {
        this.schema = schema;
        this.writable = true;
        lz4 = new LinkedHashMap<>();
        buffers = new LinkedHashMap<>();
        for(int col=0; col < schema.size(); col++) {
            ByteArrayOutputStream currentColumnSegment = new ByteArrayOutputStream(65535);
            buffers.put(col, currentColumnSegment);
            lz4.put(col, new LZ4BlockOutputStream(
                currentColumnSegment, 
                65535,
                LZ4Factory.fastestInstance().highCompressor(),
                XXHashFactory.fastestInstance().newStreamingHash32(0x9747b28c).asChecksum(), 
                true
            ));
        }
    }

    protected void flushSegment() throws IOException, IllegalAccessException {
        checkWritable(true);
        for(LZ4BlockOutputStream lz4stream: lz4.values()) {
            lz4stream.flush(); 
        }
    }

    @Override public void close() throws IOException, IllegalAccessException {
        checkWritable(true);
        this.writable = false;
        for(Entry<Integer,ByteArrayOutputStream> c: buffers.entrySet()) {
            LZ4BlockOutputStream lz4stream = lz4.get(c.getKey());
            lz4stream.close();
            byte[] buffer = c.getValue().toByteArray();
            columnar.put(c.getKey(), buffer);
            size.addAndGet(buffer.length);
        }
        buffers = null;
        lz4 = null;
    }


    @Override public InputStream open(int column) throws IOException {
        try {
            checkWritable(false);
        } catch (IllegalAccessException e) {
            throw new IOException(e); 
        }
        return new LZ4BlockInputStream(new ByteArrayInputStream(columnar.get(column)));
    }

    @Override public InputStream[] open(Integer... columns) throws IOException {
        final InputStream[] streams = new InputStream[columns.length];
        int i = 0; for(Integer col: columns) {
            streams[i++] = open(col);
        }
        return streams;
    }

    @Override public long getSize() {
        return size.get();
    }

    @Override public long getOriginalSize() {
        return originalSize.get();
    }

    private void checkWritable(boolean canBe) throws IllegalAccessException {
        if (this.writable != canBe) {
            throw new IllegalAccessException("Segment state is not valid for the operation");
        }
    }

    @Override public void write(int column, AimDataType type, byte[] value) throws IOException {
        try {
            checkWritable(true);
        } catch (IllegalAccessException e) {
            throw new IOException(e); 
        }
        if (type.equals(Aim.STRING)) {
            Pipe.write(lz4.get(column),value.length);
            originalSize.addAndGet(4);
        }
        lz4.get(column).write(value);
        originalSize.addAndGet(value.length);
    }

    @SuppressWarnings("serial")
    @Override public Integer filter(AimFilter filter, BitSet out) throws IOException {

        //TODO dig colNames
        final LinkedList<String> usedColumns = new LinkedList<String>() {{
            add("userQuizzed");
            add("post_code");
            add("api_key");
            add("timestamp");
        }};

        filter.updateFormula(usedColumns);

        final Integer[] cols = new LinkedList<Integer>() {{
            for(String colName: usedColumns) add(schema.get(colName));
        }}.toArray(new Integer[usedColumns.size()]);
        final AimDataType[] types = new LinkedList<AimDataType>() {{
            for(String colName: usedColumns) add(schema.def(colName));
        }}.toArray(new AimDataType[usedColumns.size()]);
        final byte[][] data = new byte[usedColumns.size()][];
        final InputStream[] range = open(cols);
        int length = 0;
        try {
            while (true) {
                for(int i=0;i<cols.length;i++) {
                    data[i] = Pipe.read(range[i],types[i]);
                }
                if (filter.match(data)) {
                    out.set(length);
                }
                length++;
                if (Thread.interrupted()) break;
            }
        } catch (EOFException e) {
            return length;
        } 
        throw new IOException("Filter did not process until the end of the segment");
    }

}
