package net.imagini.aim.segment;

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

import net.imagini.aim.tools.PipeUtils;
import net.imagini.aim.tools.AimFilter;
import net.imagini.aim.tools.Scanner;
import net.imagini.aim.types.Aim;
import net.imagini.aim.types.AimDataType;
import net.imagini.aim.types.AimSchema;
import net.imagini.aim.types.AimType;
import net.imagini.aim.utils.BlockStorage;

/**
 * zero-copy open methods, i.e. multiple stream readers should be able to operate without 
 * doubling the memory foot-print.
 */
abstract public class AimSegmentAbstract implements AimSegment {

    final protected AimSchema schema;
    protected LinkedHashMap<Integer,BlockStorage> columnar = new LinkedHashMap<>();
    private boolean writable;
    private LinkedHashMap<Integer,ByteBuffer> writers = null;
    private AtomicLong count = new AtomicLong(0);
    private AtomicLong size = new AtomicLong(0);
    private AtomicLong originalSize = new AtomicLong(0);
    private String keyField;

    public AimSegmentAbstract(AimSchema schema, final String keyField, Class<? extends BlockStorage> storageType) throws InstantiationException, IllegalAccessException {
        this.schema = schema;
        this.writable = true;
        this.keyField = keyField;
        writers = new LinkedHashMap<>();
        for(int col=0; col < schema.size(); col++) {
            BlockStorage blockStorage = storageType.newInstance();
            columnar.put(col, blockStorage);
            writers.put(col, blockStorage.newBlock());
        }
    }

    @Override final public long getCompressedSize() {
        return size.get();
    }

    @Override final public long getOriginalSize() {
        return originalSize.get();
    }

    final protected void checkWritable(boolean canBe) throws IllegalAccessException {
        if (this.writable != canBe) {
            throw new IllegalAccessException("Segment state is not valid for the operation");
        }
    }

    final public void appendRecord(String... values) throws IOException {
        if (values.length != schema.size()) {
            throw new IllegalArgumentException("Number of values doesn't match the number of fields in the schema");
        }
        byte[][] record = new byte[schema.size()][];
        for(int col = 0; col < schema.size() ; col++) {
            record[col] = schema.get(col).convert(values[col]);
        }
        appendRecord(record);
    }

    //FIXME this is a limitation of size of the record as well as single-threaded context
    private ByteBuffer recordBuffer = ByteBuffer.allocate(65535);
    final public void appendRecord(byte[][] record) throws IOException {
        if (record.length != schema.size()) {
            throw new IllegalArgumentException("Number of record values doesn't match the number of fields in the schema");
        }

        recordBuffer.clear();
        for(int col = 0; col < schema.size() ; col++) {
            PipeUtils.write(schema.get(col).getDataType(), record[col], recordBuffer);
        }
        recordBuffer.flip();
        appendRecord(recordBuffer);
    }

    /**
     * ByteBuffer record is a horizontal buffer where columns of a single record 
     * follow in precise order.
     */
    public void appendRecord(ByteBuffer record) throws IOException {
        try {
            checkWritable(true);
            for(int col = 0; col < schema.size() ; col++) {
                AimDataType type = schema.dataType(col);
                ByteBuffer block = writers.get(col);
                if (block.position() + record.limit() > block.capacity()) {
                    block.flip();
                    size.addAndGet(columnar.get(col).addBlock(block));
                    block.clear();
                }
                originalSize.addAndGet(
                    PipeUtils.copy(record, type, block)
                );
            }
            count.incrementAndGet();
        } catch (IllegalAccessException e1) {
            throw new IOException(e1);
        }
    }

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

    @Override final public long count() {
        return count.get();
    }

    @Override public Scanner[] wrapScanners(AimSchema subSchema) {
        final Scanner[] scanners = new Scanner[subSchema.size()];
        int i = 0; for(String colName: subSchema.names()) {
            scanners[i++] = new Scanner(columnar.get(schema.get(colName)));
        }
        return scanners;
    }

    @Override final public long count(AimFilter filter) throws IOException {
        final AimSchema subSchema;
        if (filter != null) {
            subSchema = schema.subset(filter.getColumns());
            filter.updateFormula(subSchema.names());
        } else {
            subSchema = schema.subset(new String[]{schema.name(0)});
        }

        final Scanner[] scanners = new Scanner[subSchema.size()];
        int i = 0; for(String colName: subSchema.names()) {
            scanners[i++] = new Scanner(columnar.get(schema.get(colName)));
        }

        long count = 0;
        try {
            for(int c=0; c < subSchema.size(); c++) {
                Scanner scanner = scanners[c];
                if (scanner.eof()) {
                    throw new EOFException();
                }
            }
            while(true) {
                if (filter == null || filter.matches(scanners)) {
                    count++;
                }
                for(int c=0; c< subSchema.size(); c++) {
                    AimType type = subSchema.get(c);
                    Scanner scanner = scanners[c];
                    int skipLength;
                    if (type.equals(Aim.STRING)) {
                        skipLength = 4 + scanner.asIntValue();
                    } else {
                        skipLength  = type.getDataType().getSize();
                    }
                    scanner.skip(skipLength);
                    if (scanner.eof()) {
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
    @Override final public InputStream select(final AimFilter filter, final String[] columns) throws IOException {
        try {
            checkWritable(false);
        } catch (IllegalAccessException e) {
            throw new IOException(e); 
        }
        Set<String> totalColumnSet = new HashSet<String>(Arrays.asList(columns));
        if (filter != null) totalColumnSet.addAll(Arrays.asList(filter.getColumns()));

        if (keyField != null) totalColumnSet.add(keyField);
        final AimSchema subSchema = schema.subset(totalColumnSet.toArray(new String[totalColumnSet.size()]));
        final List<Integer> allColumns = new LinkedList<Integer>(); 
        for(String colName: subSchema.names()) allColumns.add(subSchema.get(colName));
        final List<Integer> selectColumns = new LinkedList<Integer>();
        for(String colName: columns) selectColumns.add(subSchema.get(colName));
        final List<Integer> hiddenColumns = new LinkedList<Integer>(allColumns); hiddenColumns.removeAll(selectColumns);

        if (filter != null) filter.updateFormula(subSchema.names());

        final Scanner[] scanners = new Scanner[subSchema.size()];
        int i = 0; for(String colName: subSchema.names()) {
            scanners[i++] = new Scanner(columnar.get(schema.get(colName)));
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
                return checkNextByte() ? currentSelectScanner.readByte() & 0xff : -1;
            }

            private int currentSelectColumn = -1;
            private Scanner currentSelectScanner = null;
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
                            Scanner scanner = scanners[c];
                            if (scanner.eof()) {
                                return false;
                            }
                        }
                        if (filter == null || filter.matches(scanners)) {
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
