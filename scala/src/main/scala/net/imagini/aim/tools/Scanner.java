package net.imagini.aim.tools;

import java.nio.ByteBuffer;

import net.imagini.aim.types.AimType;
import net.imagini.aim.utils.BlockStorage;

public class Scanner {
    private static class Mark {
        public final Integer block;
        public final Integer position;

        public Mark(Integer block, Integer position) {
            this.block = block;
            this.position = position;
        }
    }

    private BlockStorage blockStorage;
    private Integer currentBlock = -1;
    //TODO protected plus extends ByteBuffer instead of InputStream
    public ByteBuffer scan = null;
    private Mark mark = null;

    public Scanner(BlockStorage blockStorage) {
        this.blockStorage = blockStorage;
        rewind();
    }

    // TODO mark and reset should be delegated to storage so that decompressed
    // buffers are kept in cache until marks are released
    public void mark() {
        if (!eof()) {
            this.mark = new Mark(currentBlock, scan.position());
        }
    }

    public void reset() {
        if (mark != null) {
            if (currentBlock != mark.block) {
                currentBlock = mark.block;
                decompress(mark.block);
            }
            scan.position(mark.position);
            mark = null;
        }
    }

    public int read() {
        return eof() ? -1 : scan.get();
    }

    public void skip(AimType t) {
        scan.position(scan.position() + t.getDataType().sizeOf(scan));
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
            if (!decompress(0)) {
                return true;
            }
        }
        if (scan.position() == scan.limit()) {
            if (!decompress(currentBlock + 1)) {
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

    private boolean decompress(Integer block) {
        if (this.currentBlock != block) {
            if (block > blockStorage.numBlocks() - 1) {
                return false;
            }
            this.currentBlock = block;
            scan = blockStorage.decompress(block);
        }
        scan.rewind();
        return true;
    }

}