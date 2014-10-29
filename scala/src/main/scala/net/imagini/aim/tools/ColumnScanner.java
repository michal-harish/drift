package net.imagini.aim.tools;

import java.nio.ByteBuffer;

import net.imagini.aim.types.AimDataType;
import net.imagini.aim.types.AimType;
import net.imagini.aim.utils.BlockStorage;

public class ColumnScanner {
    private static class Mark {
        public final Integer block;
        public final Integer position;

        public Mark(Integer block, Integer position) {
            this.block = block;
            this.position = position;
        }
    }

    final public AimType aimType;
    final public AimDataType dataType;
    final private BlockStorage blockStorage;
    private Integer currentBlock = -1;
    //TODO protected plus extends ByteBuffer instead of InputStream
    public ByteBuffer scan = null;
    private Mark mark = null;

    public ColumnScanner(BlockStorage blockStorage, AimType aimType) {
        this.blockStorage = blockStorage;
        this.aimType = aimType;
        this.dataType = aimType.getDataType();
        rewind();
    }

    public void mark() {
        if (!eof()) {
            blockStorage.ref(currentBlock);
            this.mark = new Mark(currentBlock, scan.position());
        }
    }

    public void reset() {
        if (mark != null) {
            if (currentBlock != mark.block) {
                currentBlock = mark.block;
                switchTo(mark.block);
            }
            scan.position(mark.position);
            blockStorage.deref(mark.block);
            mark = null;
        }
    }

    public int read() {
        return eof() ? -1 : scan.get();
    }

    public void skip() {
        scan.position(scan.position() + dataType.sizeOf(scan));
    }

    public void rewind() {
        if (currentBlock == 0 && blockStorage.numBlocks() > 0) {
            // FIXME
            scan.rewind();
            mark = null;
        } else {
            currentBlock = -1;
            scan = null;
        }
    }

    public boolean eof() {
        if (scan == null) {
            if (!switchTo(0)) {
                return true;
            }
        }
        if (scan.position() == scan.limit()) {
            if (!switchTo(currentBlock + 1)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Skips the next n bytes but the caller must know that these bytes are
     * available as this method doesn't check the block overflow
     */

    public long skip(long skipBytes) {
        if (skipBytes > scan.remaining()) {
            skipBytes = scan.remaining();
        }
        if (skipBytes > 0) {
            scan.position(scan.position() + (int) skipBytes);
        }
        return skipBytes;
    }

    private boolean switchTo(Integer block) {
        if (currentBlock != block) {
            if (currentBlock > -1) {
                blockStorage.close(currentBlock);
            }
            if (block > blockStorage.numBlocks() - 1) {
                currentBlock = -1;
                return false;
            }
            currentBlock = block;
            scan = blockStorage.open(block);
        }
        scan.rewind();
        return true;
    }

}