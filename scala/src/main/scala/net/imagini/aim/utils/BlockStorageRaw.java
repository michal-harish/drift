package net.imagini.aim.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;

public class BlockStorageRaw implements BlockStorage {

    private int size = 0;
    private LinkedList<byte[]> blocks = new LinkedList<>();

    @Override
    public int addBlock(ByteBuffer block) {
        int blockSize = block.limit();
        size += blockSize;
        blocks.add(Arrays.copyOf(block.array(), block.limit()));
        return blockSize;
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
