package net.imagini.aim.utils;

public class BlockStorageFSLZ4 extends BlockStorage {

    private static final String BASE_PATH ="/var/lib/drift";
    private final String path;

    public BlockStorageFSLZ4(String identifier) {
        this.path = BASE_PATH + "/" + identifier + "/";
    }

    @Override
    protected int blockSize() {
        return 67108864; //64Mb
    }

    @Override
    protected int storeBlock(byte[] array, int offset, int length) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int numBlocks() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long storedSize() {
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
