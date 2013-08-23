package net.imagini.aim.node;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
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
    protected AtomicLong originalSize = new AtomicLong(0);
    private LinkedHashMap<Integer,LZ4Buffer> columnar = new LinkedHashMap<>();
    private LinkedHashMap<Integer,ByteBuffer> writers = null;
    private AtomicLong count = new AtomicLong(0);
    private boolean writable;
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
            writers.put(col, ByteBuffer.allocate(Aim.LZ4_BLOCK_SIZE));
        }
    }

    final protected void checkWritable(boolean canBe) throws IllegalAccessException {
        if (this.writable != canBe) {
            throw new IllegalAccessException("Segment state is not valid for the operation");
        }
    }

    /**
     * ByteBuffer input is a horizontal buffer where columns of a single record 
     * follow in precise order.
     * @param buffer
     * @throws IOException
     */
    @Override public void append(ByteBuffer record) throws IOException {
        try {
            checkWritable(true);
            for(int col = 0; col < schema.size() ; col++) {
                AimDataType type = schema.dataType(col);
                ByteBuffer block = writers.get(col);
                if (block.position() + record.limit() > Aim.LZ4_BLOCK_SIZE) {
                    block.flip();
                    size.addAndGet(columnar.get(col).addBlock(block));
                    block.clear();
                }
                originalSize.addAndGet(
                    AimUtils.copy(record, type, block)
                );
            }
            count.incrementAndGet();
        } catch (IllegalAccessException e1) {
            throw new IOException(e1);
        }
    }

//    @Override public void append(InputStream in) throws IOException {
//        try {
//            checkWritable(true);
//            for(int col = 0; col < schema.size() ; col++) {
//                AimDataType type = schema.dataType(col);
//                ByteBuffer block = writers.get(col);
//                int valueSize = AimUtils.sizeOf(in, type);
//                if (block.position() + valueSize > Aim.LZ4_BLOCK_SIZE) {
//                    block.flip();
//                    size.addAndGet(columnar.get(col).addBlock(block));
//                    block.clear();
//                }
//                if (type.equals(Aim.STRING)) {
//                    block.putInt(valueSize);
//                }
//                AimUtils.read(in, block, valueSize);
//            }
//            count.incrementAndGet();
//        } catch (IllegalAccessException e1) {
//            throw new IOException(e1);
//        }
//    }

    @Override public void close() throws IOException, IllegalAccessException {
        checkWritable(true);
        //check open writer blocks and add them if available
        for(int col = 0; col < schema.size() ; col++) {
            ByteBuffer block = writers.get(col);
            if (block.position() > 0) {
                block.flip();
                size.addAndGet(columnar.get(col).addBlock(block));
                block.clear();
            }
        }
        this.writers = null;
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

    @Override final public Long count(AimFilter filter) throws IOException {
        final AimSchema subSchema;
        if (filter != null) {
            subSchema = schema.subset(filter.getColumns());
            filter.updateFormula(subSchema.names());
        } else {
            subSchema = schema.subset(schema.name(0));
        }

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
    @Override final public InputStream select(final AimFilter filter, final String sortColumn, final String[] columns) throws IOException {
        try {
            checkWritable(false);
        } catch (IllegalAccessException e) {
            throw new IOException(e); 
        }
        Set<String> totalColumnSet = new HashSet<String>(Arrays.asList(columns));
        if (filter != null) totalColumnSet.addAll(Arrays.asList(filter.getColumns()));

        totalColumnSet.add(sortColumn);
        final AimSchema subSchema = schema.subset(totalColumnSet.toArray(new String[totalColumnSet.size()]));
        final List<Integer> allColumns = new LinkedList<Integer>(); 
        for(String colName: subSchema.names()) allColumns.add(subSchema.get(colName));
        final List<Integer> selectColumns = new LinkedList<Integer>();
        for(String colName: columns) selectColumns.add(subSchema.get(colName));
        final List<Integer> hiddenColumns = new LinkedList<Integer>(allColumns); hiddenColumns.removeAll(selectColumns);

        if (filter != null) filter.updateFormula(subSchema.names());

        final LZ4Scanner[] scanners = new LZ4Scanner[subSchema.size()];
        int i = 0; for(String colName: subSchema.names()) {
            scanners[i++] = new LZ4Scanner(columnar.get(schema.get(colName)));
        }
        return new InputStream() {

            @Override
            public long skip(long n) throws IOException {
                if(checkNextByte()) {
                    long skipped = currentSelectScanner.skip(n);
                    //TODO break n into max column lenghts
                    read+=skipped;
                    return skipped;
                } else {
                    return 0;
                }
            }

            @Override public int read() throws IOException {
                read++; 
                return checkNextByte() ? currentSelectScanner.read() & 0xff : -1;
            }

            private int currentSelectColumn = -1;
            private LZ4Scanner currentSelectScanner = null;
            private int currentReadLength = 0;
            private int read = -1;
            private boolean checkNextByte() throws IOException {
                if (currentSelectColumn == -1) {
                    for(int c=0;c<subSchema.size();c++) {
                        if (scanners[c].eof()) return false;
                    }
                } else if (read == currentReadLength) {
                    read = -1;
                    currentSelectColumn++;
                    currentSelectScanner = (currentSelectColumn < columns.length) ? scanners[selectColumns.get(currentSelectColumn)] : null;
                    if (currentSelectScanner != null && currentSelectScanner.eof()) {
                        return false;
                    }
                }

                if (currentSelectScanner == null) {
                    read = -1;
                    currentSelectColumn = 0;
                    currentSelectScanner = scanners[selectColumns.get(currentSelectColumn)];
                    while(true) {
                        for(int c=0; c < subSchema.size(); c++) {
                            LZ4Scanner scanner = scanners[c];
                            if (scanner.eof()) {
                                return false;
                            }
                        }
                        if (filter == null || filter.match(scanners)) {
                            skipNextRecord(true);
                            break;
                        }
                        skipNextRecord(false);
                    }
                } 
                if (read == -1) {
                    read = 0;
                    AimType type = subSchema.get(selectColumns.get(currentSelectColumn));
                    if (type.equals(Aim.STRING)) {
                        currentReadLength = 4 + currentSelectScanner.asIntValue();
                    } else {
                        currentReadLength = type.getDataType().getSize();
                    }
                }
                return true;
            }
            private void skipNextRecord(boolean onlyHiddenColumns) throws IOException {
                List<Integer> colNames = onlyHiddenColumns ? hiddenColumns : allColumns;
                for(Integer c: colNames) {
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
