package net.imagini.aim.tools;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import net.imagini.aim.types.Aim;
import net.imagini.aim.types.AimDataType;
import net.imagini.aim.types.AimType;
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
    //TODO mark and reset should be delegated to storage so that decompressed buffers are kept in cache until marks are released
    public void mark() {
        if (!eof()) {
            this.mark = new Mark(currentBlock, zoom.position());
        }
    }

    @Override public void reset() {
        if (mark != null) {
            if (currentBlock != mark.block) {
                currentBlock = mark.block;
                decompress(mark.block);
            }
            zoom.position(mark.position);
            mark = null;
        }
    }
    @Override public int read() {
        return eof() ? -1 : zoom.get();
    }

    public void rewind() {
        if (currentBlock == 0 && blockStorage.numBlocks()>0) {
            //FIXME
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
     * Skips the next n bytes but the caller must know that these bytes are available
     * as this method doesn't check the block overflow
     */
    @Override public long skip(long skipBytes) {
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


    //FIXME intead of copyOfRange it should be just a view of ByteBuffer
    public byte[] asAimValue(AimType t) {
        int size = PipeUtils.sizeOf(zoom, t.getDataType());
        return Arrays.copyOfRange(zoom.array(), zoom.position(), zoom.position() + size);
    }

    public int compare(Scanner otherScanner, AimType type) {
        return compare(otherScanner.zoom, type);
    }
    /**
     * Compares the current buffer position if treated as given type with the given value
     * but does not advance
     */
    public int compare(ByteBuffer value, AimType type) {

        int ni;
        int i = zoom.position();
        int nj;
        int j = value.position();
        int n;
        int k = 0;
        if (type.equals(Aim.STRING)) {
            ni = ByteUtils.asIntValue(zoom) + 4;
            i += 4;
            nj = ByteUtils.asIntValue(value) + 4;
            j += 4;
            n = Math.min(ni, nj);
            k += 4;
        } else {
            n = ni = nj = type.getDataType().getSize();
        }
        if (ni == nj) {
            for (; k < n; k++, i++, j++) {
                int cmp = ByteUtils.compareUnisgned(zoom.get(i), value.get(j));
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
            ni = ByteUtils.asIntValue(zoom) + 4;
            i += 4;
            nj = ByteUtils.asIntValue(value) + 4;
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
            if (block > blockStorage.numBlocks() - 1) {
                return false;
            }
            this.currentBlock = block;
            zoom = blockStorage.decompress(block);
        }
        zoom.rewind();
        return true;
    }

    public String debugValue(AimType aimType) {
        return debugValue(zoom, aimType);
    }

    public String debugValue(ByteBuffer value, AimType aimType) {
        int size = aimType.getDataType().getSize();
        if (aimType.equals(Aim.STRING)) {
            size = ByteUtils.asIntValue(value) + 4;
        }
        int p1 = value.position();
        byte[] v = new byte[size];
        value.get(v);
        value.position(p1);
        return aimType.convert(v);
    }

}