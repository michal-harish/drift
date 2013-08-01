package net.imagini.aim.node;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import net.imagini.aim.Aim;
import net.imagini.aim.AimFilter;
import net.imagini.aim.AimSchema;
import net.imagini.aim.AimSegment;
import net.imagini.aim.AimType;
import net.imagini.aim.AimTypeAbstract.AimDataType;
import net.imagini.aim.AimUtils;
import net.imagini.aim.LZ4Buffer;
import net.imagini.aim.pipes.Pipe;

import org.apache.commons.io.output.ByteArrayOutputStream;

/**
 * zero-copy open methods, i.e. multiple stream readers should be able to operate without 
 * doubling the memory foot-print.
 * @author mharis
 */
public class Segment implements AimSegment {

    final protected AimSchema schema;
    final protected LinkedHashMap<Integer,LZ4Buffer> columnar = new LinkedHashMap<>();
    protected LinkedHashMap<Integer,ByteArrayOutputStream> writers = null;
    private boolean writable;
    private AtomicLong count = new AtomicLong(0);
    private AtomicLong originalSize = new AtomicLong(0);
    private AtomicLong size = new AtomicLong(0);


    //TODO public AimSegment(..mmap file)

    /**
     * Read-only Segment
     * @param data
     */
    public Segment(AimSchema schema, LinkedList<ByteArrayOutputStream> data) {
        this.schema = schema;
        this.writable = false;
        int i = 0; for(ByteArrayOutputStream b: data) {
            LZ4Buffer buffer = new LZ4Buffer();
            buffer.addBlock(b.toByteArray());
            columnar.put(i++, buffer);
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
        for(int col=0; col < schema.size(); col++) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream(65535);
            writers.put(col, buffer);
        }
    }

    final protected void checkWritable(boolean canBe) throws IllegalAccessException {
        if (this.writable != canBe) {
            throw new IllegalAccessException("Segment state is not valid for the operation");
        }
    }

    @Override final public void append(Pipe pipe) throws IOException {
        try {
            checkWritable(true);
            //System.out.println("--------");
            for(int col = 0; col < schema.size() ; col++) {
                AimDataType type = schema.dataType(col); 
                byte[] value = pipe.read(type);
                originalSize.addAndGet(write(col, type,value));
                //System.out.println(col + " " + type + " "+ type.convert(value));
            }
            count.incrementAndGet();
        } catch (IllegalAccessException e1) {
            throw new IOException(e1);
        }
    }

    protected int write(int column, AimDataType type, byte[] value) throws IOException {
        return AimUtils.write(type, value, writers.get(column));
    }


    @Override public void close() throws IOException, IllegalAccessException {
        checkWritable(true);
        this.writable = false;
        for(Entry<Integer,ByteArrayOutputStream> c: writers.entrySet()) {
            OutputStream writer = writers.get(c.getKey());
            writer.close();
            LZ4Buffer lz4buf = new LZ4Buffer();
            lz4buf.addBlock(c.getValue().toByteArray());
            columnar.put(c.getKey(), lz4buf);
            size.addAndGet(lz4buf.size());
        }
        writers = null;
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

    @Override public Long count(AimFilter filter) throws IOException {
        //TODO extract schema from the filter
        final AimSchema subSchema = schema.subset("user_quizzed");
        if (filter != null) filter.updateFormula(subSchema.names());

        final LZ4Buffer[] streams = new LZ4Buffer[subSchema.size()];
        int i = 0; for(String colName: subSchema.names()) {
            LZ4Buffer buffer = columnar.get(schema.get(colName));
            buffer.rewind(); // FIXME must be done on thread-safe instance not on the shared buffer
            streams[i++] = buffer;
        }

        long count = 0;
        try {
            for(int c=0; c < subSchema.size(); c++) {
                LZ4Buffer stream = streams[c];
                if (stream.eof()) {
                    throw new EOFException();
                }
            }
            while(true) {
                if (filter == null || filter.match(streams)) {
                    count++;
                }
                for(int c=0; c< subSchema.size(); c++) {
                    AimType type = subSchema.get(c);
                    LZ4Buffer stream = streams[c];
                    int skipLength;
                    if (type.equals(Aim.STRING)) {
                        skipLength = 4 + stream.asIntValue();
                    } else {
                        skipLength  = type.getDataType().getSize();
                    }
                    stream.skip(skipLength);
                    if (stream.eof()) {
                        throw new EOFException();
                    }
                }
            }
        } catch (EOFException e) {
            return count;
        }
    }

    /**
     * ZeroCopy
     * Not Thread-safe 
     * Filtered, Aggregate Input stream for all the selected columns in this segment.
     */
    @Override public InputStream open(final AimFilter filter, final String[] colums) throws IOException {
        try {
            checkWritable(false);
        } catch (IllegalAccessException e) {
            throw new IOException(e); 
        }
        final AimSchema subSchema = schema.subset(colums);
        //TODO detect missing filter columns 
 
        if (filter != null) filter.updateFormula(subSchema.names());

        final LZ4Buffer[] streams = new LZ4Buffer[subSchema.size()];
        int i = 0; for(String colName: subSchema.names()) {
            LZ4Buffer buffer = columnar.get(schema.get(colName));
            buffer.rewind(); // FIXME must be done on thread-safe instance not on the shared buffer
            streams[i++] = buffer;
        }
        return new InputStream() {

            @Override
            public long skip(long n) throws IOException {
                if(checkNextByte()) {
                    long skipped = currentBuffer.skip(n);
                    //TODO break n into max column lenghts
                    read+=skipped;
                    return skipped;
                } else {
                    return 0;
                }
            }

            @Override public int read() throws IOException {
                read++; 
                return checkNextByte() ? currentBuffer.read() & 0xff : -1;
            }

            private int currentColumn = -1;
            private LZ4Buffer currentBuffer = null;
            private int currentReadLength = 0;
            private int read = -1;
            private boolean checkNextByte() throws IOException {
                if (currentColumn == -1) {
                    for(int c=0;c<subSchema.size();c++) {
                        if (streams[c].eof()) return false;
                    }
                } else if (read == currentReadLength) {
                    read = -1;
                    currentColumn++;
                    currentBuffer = (currentColumn < subSchema.size()) ? streams[currentColumn] : null;
                    if (currentBuffer != null && currentBuffer.eof()) {
                        return false;
                    }
                }

                if (currentBuffer == null) {
                    read = -1;
                    currentColumn = 0;
                    currentBuffer = streams[currentColumn];
                    while(true) {
                        if (currentBuffer.eof()) {
                            return false;
                        } else if (filter == null || filter.match(streams)) {
                            break;
                        }
                        skipNextRecord();
                    }
                } 
                if (read == -1) {
                    read = 0;
                    AimType type = subSchema.get(currentColumn);
                    if (type.equals(Aim.STRING)) {
                        currentReadLength = 4 + currentBuffer.asIntValue();
                    } else {
                        currentReadLength = type.getDataType().getSize();
                    }
                }
                return true;
            }
            private void skipNextRecord() throws IOException {
                for(String colName: subSchema.names()) {
                    int c = subSchema.get(colName);
                    AimType type = subSchema.get(c);
                    int skipLength;
                    if (type.equals(Aim.STRING)) {
                        skipLength = 4 + streams[c].asIntValue();
                    } else {
                        skipLength  = type.getDataType().getSize();
                    }
                    streams[c].skip(skipLength);
                }
            }
        };
    }
}
