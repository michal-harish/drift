package net.imagini.aim.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

abstract public class BlockStorage {

    public static interface PersistentBlockStorage {}

    //private static final Logger log = LoggerFactory.getLogger(BlockStorage.class);

    protected int limit = 0;
    final protected AtomicLong storedSize = new AtomicLong(0);

    final public int store(ByteBuffer block) throws IOException {
        int stored = storeBlock(block.array(), 0, block.limit());
        storedSize.addAndGet(stored);
        return stored;
    }

    final public long getStoredSize() {
        return storedSize.get();
    }

    abstract public int blockSize();

    abstract protected int storeBlock(byte[] array, int offset, int length) throws IOException;

    abstract public View toView() throws Exception;

}