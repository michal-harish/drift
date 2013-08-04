package net.imagini.aim.node;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicLong;

import net.imagini.aim.Aim;
import net.imagini.aim.AimFilter;
import net.imagini.aim.AimSchema;
import net.imagini.aim.AimSegment;
import net.imagini.aim.AimType;
import net.imagini.aim.AimTypeAbstract.AimDataType;
import net.imagini.aim.AimUtils;
import net.imagini.aim.LZ4Buffer;
import net.imagini.aim.LZ4Buffer.LZ4Scanner;

/**
 * zero-copy open methods, i.e. multiple stream readers should be able to operate without 
 * doubling the memory foot-print.
 * @author mharis
 */
public class Segment implements AimSegment {

    final protected AimSchema schema;
    final protected LinkedHashMap<Integer,LZ4Buffer> columnar = new LinkedHashMap<>();
    //TODO move writers into the loader session context
    protected LinkedHashMap<Integer,ByteBuffer> writers = null;
    private boolean writable;
    private AtomicLong count = new AtomicLong(0);
    private AtomicLong originalSize = new AtomicLong(0);
    private AtomicLong size = new AtomicLong(0);

    /**
     * Append-only segment
     * @throws IOException 
     */
    public Segment(AimSchema schema) throws IOException {
        this.schema = schema;
        this.writable = true;
        writers = new LinkedHashMap<>();
        for(int col=0; col < schema.size(); col++) {
            columnar.put(col, new LZ4Buffer());
            writers.put(col, ByteBuffer.allocate(65535));
        }
    }

    final protected void checkWritable(boolean canBe) throws IllegalAccessException {
        if (this.writable != canBe) {
            throw new IllegalAccessException("Segment state is not valid for the operation");
        }
    }

    /**
     * ByteBuffer input is a horizontal buffer where columns of a single record 
     * follow in precise order before moving onto another record.
     * @param buffer
     * @throws IOException
     */
    final public void append(ByteBuffer input) throws IOException {
        try {
            checkWritable(true);
            for(int col = 0; col < schema.size() ; col++) {
                writers.get(col).clear();
            }
            while(input.position() < input.limit()) {
                for(int col = 0; col < schema.size() ; col++) {
                    AimDataType type = schema.dataType(col);
                    try {
                        originalSize.addAndGet(
                            AimUtils.copy(input, type, writers.get(col))
                        );
                    } catch (Exception e) {
                        System.err.println(input.position() + " " + input.limit() + " " + type);
                        throw e;
                    }
                }
                count.incrementAndGet();
            }
            for(int col = 0; col < schema.size() ; col++) {
                ByteBuffer writer = writers.get(col);
                writer.flip();
                size.addAndGet(columnar.get(col).addBlock(writer));
                writer.clear();
            }
        } catch (IllegalAccessException e1) {
            throw new IOException(e1);
        }
    }

    @Override public void close() throws IOException, IllegalAccessException {
        checkWritable(true);
        this.writable = false;
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
        final AimSchema subSchema = schema.subset("user_quizzed","api_key");
        if (filter != null) filter.updateFormula(subSchema.names());

        final LZ4Scanner[] scanners = new LZ4Scanner[subSchema.size()];
        int i = 0; for(String colName: subSchema.names()) {
            scanners[i++] = new LZ4Scanner(columnar.get(schema.get(colName)));
        }

        long count = 0;
        try {
            for(int c=0; c < subSchema.size(); c++) {
                LZ4Scanner scanner = scanners[c];
                if (scanner.eof()) {
                    throw new EOFException();
                }
            }
            while(true) {
                if (filter == null || filter.match(scanners)) {
                    count++;
                }
                for(int c=0; c< subSchema.size(); c++) {
                    AimType type = subSchema.get(c);
                    LZ4Scanner stream = scanners[c];
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
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
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

        final LZ4Scanner[] scanners = new LZ4Scanner[subSchema.size()];
        int i = 0; for(String colName: subSchema.names()) {
            scanners[i++] = new LZ4Scanner(columnar.get(schema.get(colName)));
        }
        return new InputStream() {

            @Override
            public long skip(long n) throws IOException {
                if(checkNextByte()) {
                    long skipped = currentScanner.skip(n);
                    //TODO break n into max column lenghts
                    read+=skipped;
                    return skipped;
                } else {
                    return 0;
                }
            }

            @Override public int read() throws IOException {
                read++; 
                return checkNextByte() ? currentScanner.read() & 0xff : -1;
            }

            private int currentColumn = -1;
            private LZ4Scanner currentScanner = null;
            private int currentReadLength = 0;
            private int read = -1;
            private boolean checkNextByte() throws IOException {
                if (currentColumn == -1) {
                    for(int c=0;c<subSchema.size();c++) {
                        if (scanners[c].eof()) return false;
                    }
                } else if (read == currentReadLength) {
                    read = -1;
                    currentColumn++;
                    currentScanner = (currentColumn < subSchema.size()) ? scanners[currentColumn] : null;
                    if (currentScanner != null && currentScanner.eof()) {
                        return false;
                    }
                }

                if (currentScanner == null) {
                    read = -1;
                    currentColumn = 0;
                    currentScanner = scanners[currentColumn];
                    while(true) {
                        if (currentScanner.eof()) {
                            return false;
                        } else if (filter == null || filter.match(scanners)) {
                            break;
                        }
                        skipNextRecord();
                    }
                } 
                if (read == -1) {
                    read = 0;
                    AimType type = subSchema.get(currentColumn);
                    if (type.equals(Aim.STRING)) {
                        currentReadLength = 4 + currentScanner.asIntValue();
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
                        skipLength = 4 + scanners[c].asIntValue();
                    } else {
                        skipLength  = type.getDataType().getSize();
                    }
                    scanners[c].skip(skipLength);
                }
            }
        };
    }
   
}
