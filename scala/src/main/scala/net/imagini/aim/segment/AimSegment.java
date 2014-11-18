package net.imagini.aim.segment;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import net.imagini.aim.types.AimSchema;
import net.imagini.aim.utils.BlockStorage;
import net.imagini.aim.utils.ByteUtils;
import net.imagini.aim.utils.View;

/**
 * zero-copy open methods, i.e. multiple stream readers should be able to
 * operate without doubling the memory foot-print.
 */
abstract public class AimSegment {

    final public AimSchema schema;
    protected LinkedHashMap<Integer, BlockStorage> columnar = new LinkedHashMap<>();
    private boolean writable;
    private LinkedHashMap<Integer, ByteBuffer> writers = null;
    private AtomicLong size = new AtomicLong(0);

    public AimSegment(AimSchema schema) {
        this.schema = schema;
        this.writable = false;
    }

    final public AimSegment open(Class<? extends BlockStorage> storageType, File segmentLocation) throws InstantiationException {
        String segmentId = segmentLocation.getName();
        try {
            for (File columnLocation : segmentLocation.listFiles()) {
                int col = schema.get(columnLocation.getName().split("\\.")[0]);
                String columnLocalId = segmentId + "/"
                        + schema.name(col);
                Constructor<? extends BlockStorage> c = storageType
                        .getConstructor(String.class);
                BlockStorage blockStorage = c.newInstance(columnLocalId);
                size.addAndGet(blockStorage.getStoredSize());
                columnar.put(col, blockStorage);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new InstantiationException();
        }
        return this;
    }

    final public AimSegment initStorage(Class<? extends BlockStorage> storageType) throws InstantiationException, IllegalAccessException {
        return init(storageType, "");
    }

    final public AimSegment init(
            Class<? extends BlockStorage> storageType, String regionId)
            throws InstantiationException, IllegalAccessException {
        writers = new LinkedHashMap<>();
        //TODO something better then random string for persisted segments
        String segmentId = regionId + "-" + new Random().nextInt(Integer.MAX_VALUE);
        //TODO but until random id replaced at least check if this one conflicts with existing
        for (int col = 0; col < schema.size(); col++) {
            try {
                String columnId = segmentId + "/"
                        + schema.name(col);
                Constructor<? extends BlockStorage> c = storageType
                        .getConstructor(String.class);
                BlockStorage blockStorage = c.newInstance(columnId);
                columnar.put(col, blockStorage);
                writers.put(col, ByteUtils.wrap(new byte[blockStorage.blockSize()]));
            } catch (Exception e) {
                e.printStackTrace();
                throw new InstantiationException();
            }
        }
        this.writable = true;
        return this;
    }

    final public BlockStorage getBlockStorage(int column) {
        return columnar.get(column);
    }

    final public AimSchema getSchema() {
        return schema;
    }

    final public long getCompressedSize() {
        return size.get();
    }

    public abstract AimSegment appendRecord(byte[][] record) throws IOException;

    public abstract int getRecordedSize();

    private void commitBlock(int col) throws IOException {
        ByteBuffer block = writers.get(col);
        block.flip();
        size.addAndGet(columnar.get(col).store(block));
        block.clear();

    }
    public AimSegment close() throws IOException, IllegalAccessException {
        checkWritable(true);
        // check open writer blocks and add them if available
        for (int col = 0; col < schema.size(); col++) {
            ByteBuffer block = writers.get(col);
            if (block.position() > 0) {
                commitBlock(col);
            }
        }
        this.writers = null;
        this.writable = false;
        return this;
    }

    final public AimSegment appendRecord(String... values) throws IOException {
        if (values.length != schema.size()) {
            throw new IllegalArgumentException(
                    "Number of values doesn't match the number of fields in the schema");
        }
        View[] record = new View[schema.size()];
        for (int col = 0; col < schema.size(); col++) {
            record[col] = new View(schema.get(col).convert(values[col]));
        }
        return appendRecord(record);
    }

    final public AimSegment appendRecord(View[] record) throws IOException {
        byte[][] byteRecord = new byte[record.length][];
        for (int col = 0; col < schema.size(); col++) {
            byteRecord[col] = Arrays
                    .copyOfRange(
                            record[col].array,
                            record[col].offset,
                            record[col].offset
                                    + schema.get(col).getDataType()
                                            .sizeOf(record[col]));
        }
        return appendRecord(byteRecord);
    }

    final protected AimSegment commitRecord(byte[][] record) throws IOException {
        try {
            checkWritable(true);
            for (int col = 0; col < record.length; col++) {
                ByteBuffer block = writers.get(col);
                //System.err.println("COMMIT RECORD " + block.position()  + " " + record[col].length); 
                if (block.position() + record[col].length > block.capacity()) {
                    commitBlock(col);
                }
                block.put(record[col]);
            }
            return this;
        } catch (IllegalAccessException e1) {
            throw new IOException(e1);
        }
    }

    final protected void checkWritable(boolean canBe)
            throws IllegalAccessException {
        if (this.writable != canBe) {
            throw new IllegalAccessException(
                    "Segment state is not valid for the operation");
        }
    }
}
