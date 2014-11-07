package net.imagini.aim.segment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicLong;

import net.imagini.aim.tools.ColumnScanner;
import net.imagini.aim.types.AimDataType;
import net.imagini.aim.types.AimSchema;
import net.imagini.aim.types.TypeUtils;
import net.imagini.aim.utils.BlockStorage;
import net.imagini.aim.utils.View;

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
    protected AtomicLong originalSize = new AtomicLong(0);
    private ByteBuffer recordBuffer = ByteBuffer.allocate(65535);

    public AimSegmentAbstract(AimSchema schema, Class<? extends BlockStorage> storageType) throws InstantiationException, IllegalAccessException {
        this.schema = schema;
        this.writable = true;
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

    @Override final public AimSegment appendRecord(View[]  record) throws IOException {
        recordBuffer.clear();
        for(int col = 0; col < schema.size() ; col++) {
            TypeUtils.copy(record[col].array, record[col].offset, schema.get(col).getDataType(), recordBuffer);
        }
        recordBuffer.flip();
        return appendRecord(recordBuffer);
    }

    final public AimSegment appendRecord(String... values) throws IOException {
        if (values.length != schema.size()) {
            throw new IllegalArgumentException("Number of values doesn't match the number of fields in the schema");
        }
        byte[][] record = new byte[schema.size()][];
        for(int col = 0; col < schema.size() ; col++) {
            record[col] = schema.get(col).convert(values[col]);
        }
        return appendRecord(record);
    }

    //FIXME this is a limitation of size of the record as well as single-threaded context
    final private AimSegment appendRecord(byte[][] record) throws IOException {
        if (record.length != schema.size()) {
            throw new IllegalArgumentException("Number of record values doesn't match the number of fields in the schema");
        }

        recordBuffer.clear();
        for(int col = 0; col < schema.size() ; col++) {
            TypeUtils.copy(record[col], 0, schema.get(col).getDataType(), recordBuffer);
        }
        recordBuffer.flip();
        return appendRecord(recordBuffer);
    }

    /**
     * ByteBuffer record is a horizontal buffer where columns of a single record 
     * follow in precise order.
     */
    public AimSegment appendRecord(ByteBuffer record) throws IOException {
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
                //TODO this could be done with slice instead of copy
                long oSize = TypeUtils.copy(record, type, block);
                originalSize.addAndGet(
                    oSize
                );
            }
            count.incrementAndGet();
            return this;
        } catch (IllegalAccessException e1) {
            throw new IOException(e1);
        }
    }

    @Override public AimSegment close() throws IOException, IllegalAccessException {
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
        return this;
    }

    @Override final public long count() {
        return count.get();
    }

    @Override public ColumnScanner[] wrapScanners(AimSchema subSchema) {
        final ColumnScanner[] scanners = new ColumnScanner[subSchema.size()];
        int i = 0; for(String colName: subSchema.names()) {
            scanners[i++] = new ColumnScanner(columnar.get(schema.get(colName)), schema.field(colName));
        }
        return scanners;
    }
 
}
