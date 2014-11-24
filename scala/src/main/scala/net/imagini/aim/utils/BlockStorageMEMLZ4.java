package net.imagini.aim.utils;

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

    private LinkedList<byte[]> compressedBlocks = new LinkedList<byte[]>();

    private LZ4Compressor compressor = LZ4Factory.fastestInstance()
            .highCompressor();
    private LZ4Decompressor decompressor = LZ4Factory.fastestInstance()
            .decompressor();

    byte[] compress_buffer = new byte[compressor.maxCompressedLength(blockSize())];

    public BlockStorageMEMLZ4() {
        this("blockSize=65535");
    }

    public BlockStorageMEMLZ4(String args) {
        // TODO if args then look for blockSize arg
        super();
    }

    @Override
    public int blockSize() {
        return 65535;
    }

    @Override
    public int storeBlock(byte[] array, int offset, int length) {
        int cLen = compressor.compress(array, offset, length, compress_buffer,0);
        byte[] block = new byte[cLen + 4];
        ByteUtils.putIntValue(length, block, 0);
        ByteUtils.copy(compress_buffer, 0, block, 4, cLen);
        compressedBlocks.add(block);
        return cLen;
    }


    @Override
    public View toView() throws Exception {
        return new View(new byte[blockSize()], 0, -1, 0) {
            private int block = -1;
            private byte[] decompressBuffer = new byte[blockSize()];

            @Override
            public boolean available(int numBytes) {
                if (!super.available(numBytes)) {
                    if (!openNextBlock()) {
                        return false;
                    } else {
                        return super.available(numBytes);
                    }
                } else {
                    return true;
                }
            }

            private boolean openNextBlock() {
                if (++block >= compressedBlocks.size()) {
                    return false;
                } else {
                    decompressBuffer = compressedBlocks.get(block);
                    int len = ByteUtils.asIntValue(decompressBuffer, 0);
                    decompressor.decompress(decompressBuffer, 4, array, 0, len);
                    limit = -1;
                    offset = 0;
                    size = len;
                    return true;
                }
            }
        };
    }
}
