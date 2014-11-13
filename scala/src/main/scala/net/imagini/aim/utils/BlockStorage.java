package net.imagini.aim.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

abstract public class BlockStorage {

    public static interface PersistentBlockStorage {}

    //private static final Logger log = LoggerFactory.getLogger(BlockStorage.class);

    protected List<Integer> lengths = new ArrayList<Integer>();


    final private List<AtomicInteger> blocks = new ArrayList<>();

    final public ByteBuffer newBlock() {
        return ByteUtils.wrap(new byte[blockSize()]);
    }

    final public int addBlock(ByteBuffer block) throws IOException {
        synchronized(blocks) {
            blocks.add(new AtomicInteger(0));
            lengths.add(block.limit());
            return storeBlock(block.array(), 0, block.limit());
        }
    }

    abstract protected int blockSize();

    abstract protected int storeBlock(byte[] array, int offset, int length) throws IOException;

    abstract public int numBlocks();

    abstract public long storedSize();

    abstract public long originalSize();

    abstract public InputStream openInputStream(int block) throws IOException;

}