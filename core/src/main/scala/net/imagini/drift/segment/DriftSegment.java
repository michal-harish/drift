package net.imagini.drift.segment;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import net.imagini.drift.types.DriftSchema;
import net.imagini.drift.utils.BlockStorage;
import net.imagini.drift.utils.View;

/**
 * zero-copy open methods, i.e. multiple stream readers should be able to
 * operate without doubling the memory foot-print.
 */
final public class DriftSegment {

    final public DriftSchema schema;
    protected LinkedHashMap<Integer, BlockStorage> columnar = new LinkedHashMap<>();
    private boolean writable;
    private LinkedHashMap<Integer, ByteBuffer> writers = null;
    private AtomicLong size = new AtomicLong(0);

    public DriftSegment(DriftSchema schema) {
        this.schema = schema;
        this.writable = false;
    }

    private int recordedSize = 0;

    public int getRecordedSize() {
        return recordedSize;
    }


    final public DriftSegment open(Class<? extends BlockStorage> storageType,
            File segmentLocation) throws InstantiationException {
        String segmentId = segmentLocation.getName();
        try {
            for (File columnLocation : segmentLocation.listFiles()) {
                int col = schema.get(columnLocation.getName().split("\\.")[0]);
                String columnLocalId = segmentId + "/" + schema.name(col);
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

    final public DriftSegment initStorage(
            Class<? extends BlockStorage> storageType)
            throws InstantiationException, IllegalAccessException {
        return init(storageType, "");
    }

    final public DriftSegment init(Class<? extends BlockStorage> storageType,
            String regionId) throws InstantiationException,
            IllegalAccessException {
        writers = new LinkedHashMap<>();
        // TODO something better then random string for persisted segments
        String segmentId = regionId + "-"
                + new Random().nextInt(Integer.MAX_VALUE);
        // TODO but until random id replaced at least check if this one
        // conflicts with existing
        for (int col = 0; col < schema.size(); col++) {
            try {
                String columnId = segmentId + "/" + schema.name(col);
                Constructor<? extends BlockStorage> c = storageType
                        .getConstructor(String.class);
                BlockStorage blockStorage = c.newInstance(columnId);
                columnar.put(col, blockStorage);
                ByteBuffer bb = ByteBuffer.wrap(new byte[blockStorage.blockSize()]);
                bb.order(ByteOrder.BIG_ENDIAN);
                writers.put(col, bb);
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

    final public DriftSchema getSchema() {
        return schema;
    }

    final public long getCompressedSize() {
        return size.get();
    }

    private void commitBlock(int col) throws IOException {
        ByteBuffer block = writers.get(col);
        block.flip();
        size.addAndGet(columnar.get(col).store(block));
        block.clear();

    }

    public DriftSegment close() throws IOException, IllegalAccessException {
        checkWritable(true);
        // check open writer blocks and add them if available
        for (int col = 0; col < schema.size(); col++) {
            ByteBuffer block = writers.get(col);
            if (block.position() > 0) {
                commitBlock(col);
            }
        }
        this.writers.clear();
        this.writable = false;
        return this;
    }

    final public DriftSegment commitRecord(View record) throws IOException {
        int mark = record.offset;
        try {
            checkWritable(true);
            for (int col = 0; col < schema.size(); col++) {
                int valSize = schema.get(col).sizeOf(record);
                ByteBuffer block = writers.get(col);
                if (block.position() + valSize > block.capacity()) {
                    commitBlock(col);
                }
                block.put(record.array, record.offset, valSize);
                record.offset += valSize;
                recordedSize += valSize;
            }
            return this;
        } catch (IllegalAccessException e1) {
            throw new IOException(e1);
        } finally {
            record.offset = mark;
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
