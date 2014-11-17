package net.imagini.aim.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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
 * 
 * @author mharis
 */
public class BlockStorageMEMLZ4 extends BlockStorage {

    private int originalSize = 0;
    private int compressedSize = 0;
    private LinkedList<byte[]> compressedBlocks = new LinkedList<byte[]>();

    private LZ4Compressor compressor = LZ4Factory.fastestInstance()
            .highCompressor();
    private LZ4Decompressor decompressor = LZ4Factory.fastestInstance()
            .decompressor();

    public BlockStorageMEMLZ4() {
        this("blockSize=524280");
    }

    public BlockStorageMEMLZ4(String args) {
        // TODO if args then look for blockSize arg
        super();
    }

    @Override
    public int blockSize() {
        return 524280;
    }

    @Override
    public int storeBlock(byte[] array, int offset, int length) {
        int maxCLen = compressor.maxCompressedLength(length);
        byte[] compress_buffer = new byte[maxCLen];
        int cLen = compressor.compress(array, offset, length, compress_buffer,
                0);
        int inflation = (int) (Double.valueOf(maxCLen) / Double.valueOf(cLen) / 0.01) - 100;
        if (inflation > 5) {
            // log.debug("LZ4 estimate inflation = " + inflation + " %");
            compressedBlocks.add(Arrays.copyOfRange(compress_buffer, 0, cLen));
        } else {
            compressedBlocks.add(compress_buffer);
        }

        compressedSize += cLen;
        originalSize += length;
        return cLen;
    }

    @Override
    public long originalSize() {
        return originalSize;
    }

    @Override
    public long storedSize() {
        return compressedSize;
    }

    @Override
    public int numBlocks() {
        return compressedBlocks.size();
    }

    @Override
    public InputStream openInputStreamBlock(int block) throws IOException {
        int length = lengths.get(block);
        byte[] result = new byte[length];
        decompressor.decompress(compressedBlocks.get(block), 0, result, 0, length);
        return new ByteArrayInputStream(result);
    }
}
