package net.imagini.aim.node;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import net.imagini.aim.Aim;
import net.imagini.aim.AimDataType;
import net.imagini.aim.AimSegment;
import net.imagini.aim.pipes.Pipe;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

import org.apache.commons.io.output.ByteArrayOutputStream;

/**
 * @author mharis
 */
public class Segment implements AimSegment {

    private boolean writable;
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
    public Segment(LinkedHashMap<String,ByteArrayOutputStream> data) {
        this.writable = false;
        int i = 0; for(Entry<String,ByteArrayOutputStream> c: data.entrySet()) {
            columnar.put(i++, c.getValue().toByteArray());
        }
    }

    /**
     * Append-only segment
     */
    public Segment(int numColumns) {
        this.writable = true;
        lz4 = new LinkedHashMap<>();
        buffers = new LinkedHashMap<>();
        for(int col=0; col < numColumns; col++) {
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

}
