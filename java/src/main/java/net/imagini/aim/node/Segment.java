package net.imagini.aim.node;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.imagini.aim.Aim;
import net.imagini.aim.AimFilter;
import net.imagini.aim.AimSchema;
import net.imagini.aim.AimSegment;
import net.imagini.aim.AimType;
import net.imagini.aim.AimTypeAbstract.AimDataType;
import net.imagini.aim.pipes.Pipe;

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
    private LinkedHashMap<Integer,OutputStream> writers = null;
    private AtomicLong size = new AtomicLong(0);
    private AtomicLong count = new AtomicLong(0);
    private AtomicLong originalSize = new AtomicLong(0);

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
     * @throws IOException 
     */
    public Segment(AimSchema schema) throws IOException {
        this.schema = schema;
        this.writable = true;
        writers = new LinkedHashMap<>();
        buffers = new LinkedHashMap<>();
        for(int col=0; col < schema.size(); col++) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream(65535);
            buffers.put(col, buffer);
            writers.put(col, new GZIPOutputStream(buffer,true));
            /* TODO store segment with compression flag to allow choosing between gzip & lz4
            writers.put(col, new LZ4BlockOutputStream(
                buffer, 
                65535,
                LZ4Factory.fastestInstance().highCompressor(),
                XXHashFactory.fastestInstance().newStreamingHash32(0x9747b28c).asChecksum(), 
                true
            ));*/
        }
    }

    protected void flushSegment() throws IOException, IllegalAccessException {
        checkWritable(true);
        for(OutputStream writer: writers.values()) {
            writer.flush(); 
        }
    }

    @Override public void close() throws IOException, IllegalAccessException {
        checkWritable(true);
        this.writable = false;
        for(Entry<Integer,ByteArrayOutputStream> c: buffers.entrySet()) {
            OutputStream writer = writers.get(c.getKey());
            writer.close();
            byte[] buffer = c.getValue().toByteArray();
            columnar.put(c.getKey(), buffer);
            size.addAndGet(buffer.length);
        }
        buffers = null;
        writers = null;
    }


    @Override public InputStream open(int column) throws IOException {
        try {
            checkWritable(false);
        } catch (IllegalAccessException e) {
            throw new IOException(e); 
        }
        InputStream buffer = new ByteArrayInputStream(columnar.get(column));
        return new GZIPInputStream(buffer);
        //return new LZ4BlockInputStream(buffer);
    }

    private InputStream[] open(Integer... columns) throws IOException {
        final InputStream[] streams = new InputStream[columns.length];
        int i = 0; for(Integer col: columns) {
            streams[i++] = open(col);
        }
        return streams;
    }

    @Override public long getCount() {
        return count.get();
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

    @Override public void append(Pipe pipe) throws IOException {
        try {
            checkWritable(true);
            //System.out.println("--------");
            for(int col = 0; col < schema.size() ; col++) {
                AimDataType type = schema.dataType(col); 
                byte[] value = pipe.read(type);
                write(col, type,value);
                //System.out.println(col + " " + type + " "+ type.convert(value));
            }
            count.incrementAndGet();
        } catch (IllegalAccessException e1) {
            throw new IOException(e1);
        }
    }

    private void write(int column, AimDataType type, byte[] value) throws IOException {
        try {
            checkWritable(true);
        } catch (IllegalAccessException e) {
            throw new IOException(e); 
        }
        if (type.equals(Aim.STRING)) {
            Pipe.write(writers.get(column),value.length);
            originalSize.addAndGet(4);
        }
        writers.get(column).write(value);
        originalSize.addAndGet(value.length);
    }

    @SuppressWarnings("serial")
    @Override public Integer filter(AimFilter filter, BitSet out) throws IOException {

        //TODO dig colNames
        final LinkedList<String> usedColumns = new LinkedList<String>() {{
            add("user_quizzed");
            add("post_code");
            add("api_key");
            add("timestamp");
        }};

        filter.updateFormula(usedColumns);

        final Integer[] cols = new LinkedList<Integer>() {{
            for(String colName: usedColumns) add(schema.get(colName));
        }}.toArray(new Integer[usedColumns.size()]);
        final AimDataType[] types = new LinkedList<AimType>() {{
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
