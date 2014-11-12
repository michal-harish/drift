package net.imagini.aim.tools;

import java.io.IOException;

import net.imagini.aim.types.AimDataType;
import net.imagini.aim.utils.BlockStorage;
import net.imagini.aim.utils.View;

/**
 * Not thread-safe but light-weight context object that wraps around shared
 * storage
 * 
 * @author mharis
 */
public class BlockView extends View {

    final public AimDataType dataType;
    final private BlockStorage blockStorage;
    private boolean eof = false;
    private int currentBlock = -1;
    private int markBlock = -1;
    private int markPosition = -1;
    public int count;

    public BlockView(BlockStorage blockStorage, AimDataType aimDataType) throws IOException {
        this.blockStorage = blockStorage;
        this.dataType = aimDataType;
        this.eof = !switchTo(0);
    }

    @Override public boolean eof() {
        return eof;
    }

    @Override public void rewind() throws IOException {
        super.rewind();
        this.reset();
        this.eof = !switchTo(0);
    }

    public void mark() throws IOException {
        blockStorage.ref(currentBlock);
        markBlock = currentBlock;
        markPosition = offset;
    }

    public void reset() throws IOException {
        if (markBlock != -1) {
            if (currentBlock != markBlock) {
                currentBlock = markBlock;
                eof = !switchTo(markBlock);
            }
            offset = markPosition;
            blockStorage.deref(markBlock);
            this.eof = offset >= size && markBlock >= blockStorage.numBlocks() - 1;
            markBlock = -1;
            markPosition = -1;
        }
    }

    private void checkEof() throws IOException {
        if (offset >= size) {
            if (currentBlock == blockStorage.numBlocks() - 1) {
                eof = true;
            } else {
                eof = !switchTo(currentBlock + 1);
            }
        }
    }

    public int read() throws IOException {
        if (eof) {
            return -1;
        } else {
            int result = array[offset];
            offset +=1;
            checkEof();
            return result;
        }
    }

    @Override public int skip() throws IOException {
        int skipLen = dataType.sizeOf(this);
        offset = offset + skipLen;
        count+=1;
        checkEof();
        return skipLen;
    }

    /**
     * Skips the next n bytes but the caller must know that these bytes are
     * available as this method doesn't check the block overflow TODO remove
     * this method after we have AbstractScanner.asInputStream wrapping done
     * @throws IOException 
     */

    public long skip(int skipBytes) throws IOException {
        if (skipBytes > size - offset) {
            skipBytes = size - offset;
        }
        if (skipBytes > 0) {
            offset = offset + skipBytes;
        }
        checkEof();
        return skipBytes;
    }

    private boolean switchTo(Integer block) throws IOException {
        boolean validBlock = block > -1 && block < blockStorage.numBlocks();
        if (currentBlock != block) {
            if (currentBlock > -1 && currentBlock < blockStorage.numBlocks())
                blockStorage.close(currentBlock);
            currentBlock = block;
            if (validBlock) {
                set(blockStorage.view(block));
                return true;
            } else {
                return false;
            }
        } else if (validBlock) {
            offset = 0;
            return true;
        } else {
            return false;
        }
    }

}