package net.imagini.aim.node;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import net.imagini.aim.Aim;
import net.imagini.aim.AimFilter;
import net.imagini.aim.AimSchema;
import net.imagini.aim.AimSegment;
import net.imagini.aim.AimType;
import net.imagini.aim.AimTypeAbstract.AimDataType;
import net.imagini.aim.AimUtils;
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

    final protected AimSchema schema;
    final protected LinkedHashMap<Integer,byte[]> columnar = new LinkedHashMap<>();
    protected LinkedHashMap<Integer,OutputStream> writers = null;
    private LinkedHashMap<Integer,ByteArrayOutputStream> buffers = null;
    private boolean writable;
    private AtomicLong count = new AtomicLong(0);
    private AtomicLong originalSize = new AtomicLong(0);
    private AtomicLong size = new AtomicLong(0);


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
            
            //writers.put(col, buffer);
            //writers.put(col, new GZIPOutputStream(buffer,true));
            /**/ // TODO store segment with compression flag to allow choosing between gzip & lz4
            writers.put(col, new LZ4BlockOutputStream(
                buffer, 
                65535,
                LZ4Factory.fastestInstance().highCompressor(),
                XXHashFactory.fastestInstance().newStreamingHash32(0x9747b28c).asChecksum(), 
                true
            ));/**/
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
    @Override public InputStream open(final AimFilter filter, final List<String> columns) throws IOException {
        try {
            checkWritable(false);
        } catch (IllegalAccessException e) {
            throw new IOException(e); 
        }
        //TODO detect missing filter columns 
        final List<String> usedColumns  = columns;
        if (filter != null) filter.updateFormula(usedColumns);

        final AimSchema subSchema = schema.subset(columns);
        final InputStream[] streams = new InputStream[columns.size()];
        int i = 0; for(String colName: columns) {
            InputStream buffer = new ByteArrayInputStream(columnar.get(schema.get(colName)));

            //streams[i++] = buffer;
            streams[i++] = new LZ4BlockInputStream(buffer); 
            //streams[i++] = new GZIPInputStream(buffer);
        }
        return new InputStream() {
            private int colIndex = -1;
            private int read = 0;
            final int[] sizes = new int[columns.size()];
            final byte[][] buffer = new byte[columns.size()][Aim.COLUMN_BUFFER_SIZE];
            @Override public int read() throws IOException {
                if (colIndex == -1 || read == sizes[colIndex]) {
                    read = 0;
                    if (colIndex == -1 || ++colIndex == columns.size()) {
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
                    for(String colName: columns) {
                        int c = columns.indexOf(colName);
                        AimType type = subSchema.get(c);
                        byte[] b = buffer[subSchema.get(colName)];
                        sizes[c] = AimUtils.read(streams[c],type.getDataType(), b);
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
        final List<String> usedColumns  = Arrays.asList("user_quizzed","post_code","api_key","timestamp");

        filter.updateFormula(usedColumns);

        final Integer[] cols = new LinkedList<Integer>() {{
            for(String colName: usedColumns) add(schema.get(colName));
        }}.toArray(new Integer[usedColumns.size()]);
        final AimDataType[] types = new LinkedList<AimType>() {{
            for(String colName: usedColumns) add(schema.def(colName));
        }}.toArray(new AimDataType[usedColumns.size()]);
        final byte[][] data = new byte[usedColumns.size()][];
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
