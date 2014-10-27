package net.imagini.aim.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;

public class BlockStorageRaw implements BlockStorage {

    private int size = 0;

    //FIXME ByteBuffer refernce
    private LinkedList<byte[]> blocks = new LinkedList<>();


    public void clear() {
        blocks.clear();
        size = 0;
    }

    @Override
    public int addBlock(ByteBuffer block) {
        size += block.limit();
        blocks.add(Arrays.copyOf(block.array(), block.limit()));
        return block.limit();
    }

    @Override
    public int numBlocks() {
        return blocks.size();
    }

    @Override
    public long compressedSize() {
        return size;
    }

    @Override
    public long originalSize() {
        return size;
    }

    @Override
    public ByteBuffer decompress(int block) {
        return ByteBuffer.wrap(blocks.get(block));
    }

    @Override
    public ByteBuffer newBlock() {
        return ByteBuffer.allocate(65535 * 256);
    }

}
