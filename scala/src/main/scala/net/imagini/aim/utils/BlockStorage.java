package net.imagini.aim.utils;

import java.nio.ByteBuffer;

public interface BlockStorage {

    int addBlock(ByteBuffer block);

    int numBlocks();

    long compressedSize();

    long originalSize();

    ByteBuffer decompress(int block);

    ByteBuffer newBlock();
}