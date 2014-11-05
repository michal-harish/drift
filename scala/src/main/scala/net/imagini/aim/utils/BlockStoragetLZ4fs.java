package net.imagini.aim.utils;

public class BlockStoragetLZ4fs extends BlockStorage {

    //var/lib/drift/..

    @Override
    protected byte[] allocateBlock() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected int compressBlock(byte[] array, int offset, int length) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int numBlocks() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long compressedSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long originalSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    protected byte[] decompress(int block) {
        // TODO Auto-generated method stub
        return null;
    }

}
