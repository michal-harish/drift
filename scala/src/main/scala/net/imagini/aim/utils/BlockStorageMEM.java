package net.imagini.aim.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedList;

public class BlockStorageMEM extends BlockStorage {

    private LinkedList<byte[]> blocks = new LinkedList<byte[]>();

    public BlockStorageMEM() {
        this("blockSize=1048576");
    }

    public BlockStorageMEM(String args) {
        // TODO if args then look for blockSize arg
        super();
    }

    @Override
    protected int blockSize() {
        return 1048576; // 1Mb
    }

    @Override
    protected int storeBlock(byte[] array, int offset, int length) {
        blocks.add(Arrays.copyOfRange(array, offset, length));
        return array.length;
    }

    @Override
    public int numBlocks() {
        return blocks.size();
    }

    @Override
    public long storedSize() {
        return originalSize();
    }

    @Override
    public long originalSize() {
        int size = 0;
        for (byte[] a : blocks) {
            size += a.length;
        }
        return size;
    }

    @Override 
    public InputStream openInputStream(int block) throws IOException {
        return new ByteArrayInputStream(blocks.get(block));
    }
}
