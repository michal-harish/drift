package net.imagini.aim.tools;

import java.nio.ByteBuffer;

import net.imagini.aim.types.Aim;
import net.imagini.aim.types.AimDataType;
import net.imagini.aim.utils.BlockStorage;
import net.imagini.aim.utils.ByteUtils;

//TODO rewrite in scala
public class Scanner {
    private BlockStorage blockStorage;
    private Integer currentBlock = -1;
    private ByteBuffer zoom;
    private int maxBlock;

    public Scanner(BlockStorage blockStorage) {
        this.blockStorage = blockStorage;
        this.maxBlock = blockStorage.numBlocks() - 1;
        rewind();
    }

    public void rewind() {
        currentBlock = -1;
        zoom = null;
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
     * Skips the next n bytes but the caller must know that these bytes are available
     * as this method doesn't check the block overflow
     */
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
        return ByteUtils.getIntValue(zoom);
    }

    /**
     * Reads a byte and advances the position
     */
    public byte readByte() {
        return zoom.get();
    }

    /**
     * Compares the current buffer position if treated as given type with the given value
     * but does not advance
     */
    public int compare(ByteBuffer value, AimDataType type) {
        int ni;
        int i = zoom.position();
        int nj;
        int j = 0;
        int n;
        if (type.equals(Aim.STRING)) {
            ni = ByteUtils.getIntValue(zoom) + 4;
            i += 4;
            nj = ByteUtils.getIntValue(value) + 4;
            j += 4;
            n = Math.min(ni, nj);
        } else {
            n = ni = nj = type.getSize();
        }
        if (ni == nj) {
            for (; j < n; i++, j++) {
                int cmp = Byte.compare(zoom.get(i), value.get(j));
                if (cmp != 0) {
                    return cmp;
                }
            }
        }
        return ni - nj;
    }

    /**
     * Checks if the current buffer position if treated as given type would contain the given value
     * but does not advance
     */
    public boolean contains(ByteBuffer value, AimDataType type) {
        int ni;
        int i = zoom.position();
        int nj;
        int j = 0;
        if (type.equals(Aim.STRING)) {
            ni = ByteUtils.getIntValue(zoom) + 4;
            i += 4;
            nj = ByteUtils.getIntValue(value) + 4;
            j += 4;
        } else {
            ni = nj = type.getSize();
        }
        if (nj > ni) {
            return false;
        } else {
            ni += zoom.position();
            int v = j;
            for (; i < ni; i++) {
                byte b = zoom.get(i);
                if (value.get(v) != b) {
                    v = j;
                } else if (++v == value.limit()) {
                    return true;
                }
            }
            return false;
        }
    }

    private boolean decompress(Integer block) {
        if (this.currentBlock != block) {
            if (block > maxBlock) {
                return false;
            }
            this.currentBlock = block;
            zoom = blockStorage.decompress(block);
        }
        zoom.rewind();
        return true;
    }

}