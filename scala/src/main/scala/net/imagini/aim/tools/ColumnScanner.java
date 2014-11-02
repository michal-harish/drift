package net.imagini.aim.tools;

import java.io.EOFException;
import java.nio.ByteBuffer;

import net.imagini.aim.types.AimDataType;
import net.imagini.aim.types.AimType;
import net.imagini.aim.utils.BlockStorage;

/**
 * Not thread-safe but light-weight context object that wraps around shared
 * storage
 * 
 * @author mharis
 */
public class ColumnScanner {

    final public AimType aimType;
    final public AimDataType dataType;
    final private BlockStorage blockStorage;
    private Boolean eof = false;
    private Integer currentBlock = -1;
    volatile private ByteBuffer buffer = null;
    private Integer markBlock = -1;
    private Integer markPosition = -1;

    public ColumnScanner(BlockStorage blockStorage, AimType aimType) {
        this.blockStorage = blockStorage;
        this.aimType = aimType;
        this.dataType = aimType.getDataType();
        this.eof = !switchTo(0);
    }

    public ByteBuffer buffer() throws EOFException {
        return buffer;
    }

    public void rewind() {
        this.reset();
        this.eof = !switchTo(0);
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
                eof = !switchTo(markBlock);
            }
            buffer.position(markPosition);
            blockStorage.deref(markBlock);
            this.eof = buffer.position() >= buffer.limit()
                    && markBlock >= blockStorage.numBlocks() - 1;
            markBlock = -1;
            markPosition = -1;
        }
    }

    public boolean eof() {
        return eof;
    }

    private void checkEof() {
        if (buffer.position() >= buffer.limit()) {
            if (currentBlock == blockStorage.numBlocks() - 1) {
                eof = true;
            } else {
                eof = !switchTo(currentBlock + 1);
            }
        }
    }

    public int read() {
        if (eof) {
            return -1;
        } else {
            int result = buffer.get();
            checkEof();
            return result;
        }
    }

    public void skip() {
        buffer.position(buffer.position() + dataType.sizeOf(buffer));
        checkEof();
    }

    /**
     * Skips the next n bytes but the caller must know that these bytes are
     * available as this method doesn't check the block overflow TODO remove
     * this method after we have AbstractScanner.asInputStream wrapping done
     */

    public long skip(long skipBytes) {
        if (skipBytes > buffer.remaining()) {
            skipBytes = buffer.remaining();
        }
        if (skipBytes > 0) {
            buffer.position(buffer.position() + (int) skipBytes);
        }
        checkEof();
        return skipBytes;
    }

    private boolean switchTo(Integer block) {
        boolean validBlock = block > -1 && block < blockStorage.numBlocks();
        if (currentBlock != block) {
            if (currentBlock > -1 && currentBlock < blockStorage.numBlocks())
                blockStorage.close(currentBlock);
            currentBlock = block;
            if (validBlock) {
                buffer = blockStorage.open(block);
                return true;
            } else {
                return false;
            }
        } else if (validBlock) {
            buffer.rewind();
            return true;
        } else {
            return false;
        }
    }

}