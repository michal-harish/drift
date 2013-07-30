package net.imagini.aim.node;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.BitSet;
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

    /**
     * ZeroCopy, Not Thread-safe
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
            private int colIndex = -1;
            private int read = 0;
            final int[] sizes = new int[subSchema.size()];
            final byte[][] buffer = new byte[subSchema.size()][Aim.COLUMN_BUFFER_SIZE];
            @Override public int read() throws IOException {
                if (colIndex == -1 || read == sizes[colIndex]) {
                    read = 0;
                    if (colIndex == -1 || ++colIndex == subSchema.size()) {
                        colIndex = 0;
                        while(true) {
                            if (!readNextRecord()) {
                                return -1;
                            }
                            if (filter == null || filter.match(buffer)) {
                                break;
                            }
                        }
                    }
                }
                return buffer[colIndex][read++] & 0xff;
            }
            private boolean readNextRecord() throws IOException {
                try {
                    for(String colName: subSchema.names()) {
                        int c = subSchema.get(colName);
                        AimType type = subSchema.get(c);
                        byte[] b = buffer[subSchema.get(colName)];
                        sizes[c] = streams[c].read(type.getDataType(), b);
                    }
                    return true;
                } catch (EOFException e) {
                    return false;
                }
            }
        };
    }

    @SuppressWarnings("serial")
    @Override final public Integer filter(AimFilter filter, BitSet out) throws IOException {

        //TODO dig colNames
        final String[] usedColumns  = new String[]{"timestamp","user_quizzed","post_code","api_key","user_uid"};
        filter.updateFormula(usedColumns);

        final Integer[] cols = new LinkedList<Integer>() {{
            for(String colName: usedColumns) add(schema.get(colName));
        }}.toArray(new Integer[usedColumns.length]);
        final AimDataType[] types = new LinkedList<AimDataType>() {{
            for(String colName: usedColumns) {
                AimType t = schema.def(colName);
                add(t.getDataType());
            }
        }}.toArray(new AimDataType[usedColumns.length]);
        final byte[][] data = new byte[usedColumns.length][];
        final InputStream range = open(null, usedColumns);
        int length = 0;
        try {
            while (true) {
                for(int i=0;i<cols.length;i++) {
                    data[i] = Pipe.read(range,types[i]);
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
