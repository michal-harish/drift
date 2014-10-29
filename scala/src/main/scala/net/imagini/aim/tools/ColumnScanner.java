package net.imagini.aim.tools;

import java.io.EOFException;
import java.nio.ByteBuffer;

import net.imagini.aim.types.AimDataType;
import net.imagini.aim.types.AimType;
import net.imagini.aim.utils.BlockStorage;

/**
 * Not thread-safe but light-weight context object that wraps around shared storage
 * @author mharis
 */
public class ColumnScanner {

    final public AimType aimType;
    final public AimDataType dataType;
    final private BlockStorage blockStorage;
    private Integer currentBlock = -1;
    private ByteBuffer buffer = null;
    private Integer markBlock = -1;
    private Integer markPosition = -1; 

    public ColumnScanner(BlockStorage blockStorage, AimType aimType) {
        this.blockStorage = blockStorage;
        this.aimType = aimType;
        this.dataType = aimType.getDataType();
    }

    public ByteBuffer buffer() throws EOFException {
        return buffer;
    }

    public void mark() {
        blockStorage.ref(currentBlock);
        markBlock = currentBlock;
        markPosition = buffer.position();
    }

    public void reset() {
        if (markBlock != -1) {
            if (currentBlock != markBlock) {
                currentBlock = markBlock;
                switchTo(markBlock);
            }
            buffer.position(markPosition);
            blockStorage.deref(markBlock);
            markBlock = -1;
            markPosition = -1;
        }
    }

    public int read() {
        return eof() ? -1 : buffer.get();
    }

    public void skip() {
        buffer.position(buffer.position() + dataType.sizeOf(buffer));
    }

    public boolean eof() {
        if (buffer == null) {
            return !switchTo(0);
        } else if (buffer.position() == buffer.limit()) {
            return !switchTo(currentBlock + 1);
        } else {
            return false;
        }
    }

    /**
     * Skips the next n bytes but the caller must know that these bytes are
     * available as this method doesn't check the block overflow
     * TODO remove this method after we have AbstractScanner.asInputStream wrapping done
     */

    public long skip(long skipBytes) {
        if (skipBytes > buffer.remaining()) {
            skipBytes = buffer.remaining();
        }
        if (skipBytes > 0) {
            buffer.position(buffer.position() + (int) skipBytes);
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
            buffer = blockStorage.open(block);
        }
        buffer.rewind();
        return true;
    }

}