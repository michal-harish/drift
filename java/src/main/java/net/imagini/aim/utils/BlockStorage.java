package net.imagini.aim.utils;

import java.nio.ByteBuffer;

public interface BlockStorage {

  int addBlock(ByteBuffer block);
  int numBlocks();
  long compressedSize();
  ByteBuffer decompress(int block);
  ByteBuffer createWriterBuffer();
}