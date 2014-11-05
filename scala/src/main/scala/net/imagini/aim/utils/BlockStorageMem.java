package net.imagini.aim.utils;

import java.util.Arrays;
import java.util.LinkedList;

public class BlockStorageMem  extends BlockStorage {

    final public static Integer MEM_BLOCK_SIZE = 65535;
    private LinkedList<byte[]> blocks = new LinkedList<byte[]>();

    @Override
    protected byte[] allocateBlock() {
        return new byte[MEM_BLOCK_SIZE];
    }

    @Override
    protected int compressBlock(byte[] array, int offset, int length) {
        int inflation = (int)(Double.valueOf(MEM_BLOCK_SIZE) / Double.valueOf(length-offset) / 0.01) - 100;
        System.err.println(length + " inflation: " + inflation + " %");
        blocks.add(Arrays.copyOfRange(array, offset, length));
//        if (offset >0 || inflation > 0.5) {
//            blocks.add(Arrays.copyOfRange(array, offset, length));
//        } else {
//            blocks.add(array);
//        }
        return array.length;
    }

    @Override
    public int numBlocks() {
        return blocks.size();
    }

    @Override
    public long compressedSize() {
        return originalSize();
    }

    @Override
    public long originalSize() {
        int size = 0;
        for(byte[] a: blocks) {
            size += a.length;
        }
        return size;
    }

    @Override
    protected byte[] decompress(int block) {
        return blocks.get(block);
    }

}
