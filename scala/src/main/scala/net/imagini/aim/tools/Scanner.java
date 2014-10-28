package net.imagini.aim.tools;

import java.io.InputStream;
import java.nio.ByteBuffer;

import net.imagini.aim.types.AimType;
import net.imagini.aim.types.TypeUtils;
import net.imagini.aim.utils.BlockStorage;
import net.imagini.aim.utils.ByteUtils;

public class Scanner extends InputStream {
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
    protected ByteBuffer zoom = null;
    private Mark mark = null;

    public Scanner(BlockStorage blockStorage) {
        this.blockStorage = blockStorage;
        rewind();
    }

    // TODO mark and reset should be delegated to storage so that decompressed
    // buffers are kept in cache until marks are released
    public void mark() {
        if (!eof()) {
            this.mark = new Mark(currentBlock, zoom.position());
        }
    }

    @Override
    public void reset() {
        if (mark != null) {
            if (currentBlock != mark.block) {
                currentBlock = mark.block;
                decompress(mark.block);
            }
            zoom.position(mark.position);
            mark = null;
        }
    }

    @Override
    public int read() {
        return eof() ? -1 : zoom.get();
    }

    public void skip(AimType t) {
        zoom.position(zoom.position() + t.getDataType().sizeOf(zoom));
    }

    public void rewind() {
        if (currentBlock == 0 && blockStorage.numBlocks() > 0) {
            // FIXME
            zoom.rewind();
            mark = null;
        } else {
            currentBlock = -1;
            zoom = null;
        }
    }

    public boolean eof() {
        if (zoom == null) {
            if (!decompress(0)) {
                return true;
            }
        }
        if (zoom.position() == zoom.limit()) {
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
    @Override
    public long skip(long skipBytes) {
        if (skipBytes > zoom.remaining()) {
            skipBytes = zoom.remaining();
        }
        if (skipBytes > 0) {
            zoom.position(zoom.position() + (int) skipBytes);
        }
        return skipBytes;
    }

    /**
     * Reads an integer at the current position but does not advance
     */
    public int asIntValue() {
        return ByteUtils.asIntValue(zoom);
    }

    public ByteBuffer scan() {
        return zoom;
    }

    public ByteBuffer slice() {
        // TODO slice as well as mark should leave underlying segments
        // uncompressed as they called for back-referencing values
        return zoom.slice();
    }

    public int compare(Scanner otherScanner, AimType type) {
        return TypeUtils.compare(zoom, otherScanner.zoom, type);
    }

    public int compare(ByteBuffer value, AimType type) {
        return TypeUtils.compare(zoom, value, type);
    }

    public boolean equals(Scanner otherScanner, AimType type) {
        return compare(otherScanner, type) == 0;
    }

    public boolean equals(ByteBuffer value, AimType type) {
        return compare(value, type) == 0;
    }

    public boolean contains(ByteBuffer value, AimType type) {
        return TypeUtils.contains(zoom, value, type);
    }

    private boolean decompress(Integer block) {
        if (this.currentBlock != block) {
            if (block > blockStorage.numBlocks() - 1) {
                return false;
            }
            this.currentBlock = block;
            zoom = blockStorage.decompress(block);
        }
        zoom.rewind();
        return true;
    }

}