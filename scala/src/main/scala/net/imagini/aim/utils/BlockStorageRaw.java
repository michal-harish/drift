package net.imagini.aim.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;

public class BlockStorageRaw implements BlockStorage {

    private int size = 0;
    private LinkedList<byte[]> blocks = new LinkedList<>();

    public void clear() {
        blocks.clear();
        size = 0;
    }

    public int addBlock(byte[] block) {
        size += block.length;
        blocks.add(block);
        return block.length;
    }

    @Override
    public int addBlock(ByteBuffer block) {
        return addBlock(Arrays.copyOf(block.array(), block.limit()));
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
