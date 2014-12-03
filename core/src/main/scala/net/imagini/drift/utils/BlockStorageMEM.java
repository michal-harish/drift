package net.imagini.drift.utils;

import java.util.Arrays;
import java.util.LinkedList;

public class BlockStorageMEM extends BlockStorage {

    private LinkedList<byte[]> blocks = new LinkedList<byte[]>();

    public BlockStorageMEM() {
        this("blockSize=8192");
    }

    public BlockStorageMEM(String args) {
        // TODO if args then look for blockSize arg
        super();
    }

    @Override
    public int blockSize() {
        return 8192; // 8Kb
    }

    @Override
    protected int storeBlock(byte[] array, int offset, int length) {
        blocks.add(Arrays.copyOfRange(array, offset, length));
        return array.length;
    }

    @Override
    public View toView() throws Exception {
        return new View(new byte[0], 0, -1, 0) {
            private int block = -1;

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
                if (++block >= blocks.size()) {
                    return false;
                } else {
                    array = blocks.get(block);
                    limit = 0;
                    offset = 0;
                    size = array.length;
                    return true;
                }
            }
        };
    }
}
