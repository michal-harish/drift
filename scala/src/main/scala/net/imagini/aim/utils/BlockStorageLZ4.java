package net.imagini.aim.utils;

import java.util.Arrays;
import java.util.LinkedList;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Decompressor;
import net.jpountz.lz4.LZ4Factory;

/**
 * This is an lz4 buffer for ZeroCopy filtering and scanning.
 * 
 * It is not Thread-Safe as it is a shared storage. For Thread-Safe operations
 * over an instance of LZ4Buffer use Scanner instance
 * 
 * Although we've tried to use direct buffers, it seems that it doesn't give
 * that much performance - the zero copy approach itself has much larger effect
 * - so given the headaches with memory leaks we user array-backed ByteBuffer.
 * 
 * @author mharis
 */
public class BlockStorageLZ4 extends BlockStorage {

    final public static Integer LZ4_BLOCK_SIZE = 524280;
    private int originalSize = 0;
    private int compressedSize = 0;
    private LinkedList<byte[]> compressedBlocks = new LinkedList<byte[]>();
    private LinkedList<Integer> lengths = new LinkedList<Integer>();

    private LZ4Compressor compressor = LZ4Factory.fastestInstance().highCompressor();
    private LZ4Decompressor decompressor = LZ4Factory.fastestInstance().decompressor();

    @Override
    public byte[] allocateBlock() {
        return new byte[LZ4_BLOCK_SIZE];
    }

    @Override
    public int compressBlock(byte[] array, int offset, int length) {
        int maxCLen = compressor.maxCompressedLength(length);
        byte[] compress_buffer = new byte[maxCLen];
        int cLen = compressor.compress(array, offset, length, compress_buffer,0);
        int inflation = (int)(Double.valueOf(maxCLen) / Double.valueOf(cLen) / 0.01) - 100;
        if (inflation > 5) {
            //System.err.println("LZ4 estimate inflation = " + inflation + " %");
            compressedBlocks.add(Arrays.copyOfRange(compress_buffer, 0, cLen));
        } else {
            compressedBlocks.add(compress_buffer);
        }

        lengths.add(length);
        compressedSize += cLen;
        originalSize += length;
        return cLen;
    }

    @Override
    public long originalSize() {
        return originalSize;
    }

    @Override
    public long compressedSize() {
        return compressedSize;
    }

    @Override
    public int numBlocks() {
        return compressedBlocks.size();
    }

    @Override
    protected byte[] decompress(int block) {
        int length = lengths.get(block);
        byte[] result = new byte[length];
        decompressor.decompress(compressedBlocks.get(block), 0, result, 0, length);
        return result;
    }

}
