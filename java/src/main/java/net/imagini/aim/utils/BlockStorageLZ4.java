package net.imagini.aim.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Decompressor;
import net.jpountz.lz4.LZ4Factory;

/**
 * This is an lz4 buffer for ZeroCopy filtering and scanning.
 * 
 * It is not Thread-Safe as it is a shared storage.
 * For Thread-Safe operations over an instance of LZ4Buffer use LZ4Scanner.
 * 
 * Although we've tried to use direct buffers, it seems that it doesn't
 * give that much performance - the zero copy approach itself has much larger effect -
 * so given the headaches with memory leaks we user array-backed ByteBuffer.
 * 
 * @author mharis
 */
public class BlockStorageLZ4 implements BlockStorage {

    final public static Integer LZ4_BLOCK_SIZE = 524280;
    private int originalSize = 0;
    private int compressedSize = 0;
    private LinkedList<byte[]> compressedBlocks = new LinkedList<byte[]>();
    private LinkedList<Integer> lengths = new LinkedList<Integer>();

    private Object compress_lock = new Object();
    private byte[] compress_buffer = new byte[65535];
    private LZ4Compressor compressor = LZ4Factory.fastestInstance().highCompressor();
    private LZ4Decompressor decompressor = LZ4Factory.fastestInstance().decompressor();
    private byte[] decompress_buffer = new byte[65535];

    @Override public ByteBuffer newBlock() {
        return ByteBuffer.allocate(LZ4_BLOCK_SIZE);
    }
    @Override public int addBlock(ByteBuffer block) {

        int blockLength = block.limit();
        int maxCLen = compressor.maxCompressedLength(blockLength);
        synchronized(compress_lock) {
            if (compress_buffer.length < maxCLen) {
                compress_buffer = new byte[maxCLen+65535];
            }
            int cLen = compressor.compress(block.array(), 0, blockLength, compress_buffer, 0);
            compressedBlocks.add(Arrays.copyOfRange(compress_buffer, 0, cLen));
            lengths.add(blockLength);
            compressedSize += cLen;
            originalSize += blockLength;
            return cLen;
        }
    }

    @Override public long originalSize() {
        return originalSize;
    }
    @Override public long compressedSize() {
        return compressedSize;
    }
    @Override public int numBlocks() {
        return compressedBlocks.size();
    }

    @Override public ByteBuffer decompress(int block) {
        int length = lengths.get(block);
        if (decompress_buffer == null || decompress_buffer.length<length) {
            decompress_buffer = new byte[length];
        }
        decompressor.decompress(
            compressedBlocks.get(block), 
            0, decompress_buffer, 0, length
        );

        ByteBuffer zoom = ByteBuffer.wrap(decompress_buffer,0,length);
        /**
         * Direct malloc() 
         *
        zoom = ByteBuffer.allocateDirect(length);
        zoom.put(decompress_buffer, 0, length);
        zoom.flip();*/

        zoom.order(ByteUtils.ENDIAN);
        return zoom;

    }

}
