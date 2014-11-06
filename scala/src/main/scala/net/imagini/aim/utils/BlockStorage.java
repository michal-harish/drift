package net.imagini.aim.utils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

abstract public class BlockStorage {

    final public int hotSpotBlock = -1;
    final public ByteBuffer hotSpot = null;
    protected LinkedList<Integer> lengths = new LinkedList<Integer>();

    final public ByteBuffer newBlock() {
        return ByteUtils.wrap(allocateBlock());
    }

    final private ConcurrentMap<Integer, byte[]> cache = new ConcurrentHashMap<>();

    final private LinkedList<AtomicInteger> blocks = new LinkedList<>();

    final public int addBlock(ByteBuffer block) {
        return addBlock(block.array(), 0, block.limit());
    }

    final public int addBlock(byte[] array, int offset, int length) {
        synchronized(blocks) {
            blocks.add(new AtomicInteger(0));
            lengths.add(length);
            return compressBlock(array, offset, length);
        }
    }

    //TODO provide a timeout
    final public void ref(int block) {
        synchronized(blocks) {
            if ( blocks.get(block).getAndIncrement() == 0) {
                //log.debug("adding cache " + block + " refCount= " + blocks.get(block).get());
                cache.put(block, decompress(block));
            }
        }
    }
    final public void deref(int block) {
        synchronized(blocks) {
            if (blocks.get(block).decrementAndGet() == 0) {
                //log.debug("removing cache " + block + " refCount= " + blocks.get(block).get());
                cache.remove(block);
            }
        }
    }


    final public ByteBuffer open(int block) {
        ref(block);
        ByteBuffer zoom = ByteBuffer.wrap(cache.get(block), 0, lengths.get(block));
        /**
         * Direct malloc() 
         *
        zoom = ByteBuffer.allocateDirect(raw.length);
        zoom.put(decompress_buffer, 0, length);
        zoom.flip();*/

        zoom.order(ByteOrder.BIG_ENDIAN);
        return zoom;

    }

    final public void close(int block) {
        deref(block);
    }

    abstract protected byte[] allocateBlock();

    abstract protected int compressBlock(byte[] array, int offset, int length);

    abstract public int numBlocks();

    abstract public long compressedSize();

    abstract public long originalSize();

    abstract protected byte[] decompress(int block);


}