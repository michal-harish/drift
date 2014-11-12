package net.imagini.aim.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class BlockStorage {
    public static interface PersistentBlockStorage {

    }

    private static final Logger log = LoggerFactory.getLogger(BlockStorage.class);

    protected List<Integer> lengths = new ArrayList<Integer>();

    //FIXME ref and deref need bullet-proofing - this is not just optimisaion but also group-filter dependency
    //TODO provide also timeout for ref(.)
    final private boolean memOptimisation = false;

    final private ConcurrentMap<Integer, byte[]> cache = new ConcurrentHashMap<>();

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

    final public void ref(int block) throws IOException {
        if (memOptimisation) synchronized(blocks) {
            if ( blocks.get(block).getAndIncrement() == 0) {
                log.debug("adding cache " + block + " refCount= " + blocks.get(block).get());
                cache.put(block, load(block));
            }
        }
    }
    final public void deref(int block) {
        if (memOptimisation) synchronized(blocks) {
            if (blocks.get(block).decrementAndGet() == 0) {
                log.debug("removing cache " + block + " refCount= " + blocks.get(block).get());
                cache.remove(block);
            }
        }
    }

    final public View view(int block) throws IOException {
        if (memOptimisation) {
            ref(block);
            return new View(cache.get(block), 0, lengths.get(block));
        } else {
            byte[] blockData = load(block);
            return new View(blockData, 0, lengths.get(block));
        }
    }

    final public void close(int block) {
        deref(block);
    }

    abstract protected int blockSize();

    abstract protected int storeBlock(byte[] array, int offset, int length) throws IOException;

    abstract protected byte[] load(int block) throws IOException;

    abstract public int numBlocks();

    abstract public long storedSize();

    abstract public long originalSize();



}