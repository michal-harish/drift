package net.imagini.aim.tools;

import net.imagini.aim.types.AimDataType;
import net.imagini.aim.types.AimType;
import net.imagini.aim.utils.BlockStorage;
import net.imagini.aim.utils.View;

/**
 * Not thread-safe but light-weight context object that wraps around shared
 * storage
 * 
 * @author mharis
 */
public class ColumnScanner /*extends View*/ {

    final public AimType aimType;
    final public AimDataType dataType;
    final private BlockStorage blockStorage;
    public boolean eof = false;
    private int currentBlock = -1;
    public View view = null;
    private int markBlock = -1;
    private int markPosition = -1;

    public ColumnScanner(BlockStorage blockStorage, AimType aimType) {
        this.blockStorage = blockStorage;
        this.aimType = aimType;
        this.dataType = aimType.getDataType();
        this.eof = !switchTo(0);
    }

    public void rewind() {
        this.reset();
        this.eof = !switchTo(0);
    }

    public void mark() {
        blockStorage.ref(currentBlock);
        markBlock = currentBlock;
        markPosition = view.offset;
    }

    public void reset() {
        if (markBlock != -1) {
            if (currentBlock != markBlock) {
                currentBlock = markBlock;
                eof = !switchTo(markBlock);
            }
            view.offset = markPosition;
            blockStorage.deref(markBlock);
            this.eof = view.offset >= view.size && markBlock >= blockStorage.numBlocks() - 1;
            markBlock = -1;
            markPosition = -1;
        }
    }

    private void checkEof() {
        if (view.offset >= view.size) {
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
            int result = view.array[view.offset];
            view.offset +=1;
            checkEof();
            return result;
        }
    }

    public void skip() {
        view.offset = view.offset + dataType.sizeOf(view);
        checkEof();
    }

    /**
     * Skips the next n bytes but the caller must know that these bytes are
     * available as this method doesn't check the block overflow TODO remove
     * this method after we have AbstractScanner.asInputStream wrapping done
     */

    public long skip(int skipBytes) {
        if (skipBytes > view.size - view.offset) {
            skipBytes = view.size - view.offset;
        }
        if (skipBytes > 0) {
            view.offset = view.offset + skipBytes;
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
                view = blockStorage.view(block);
                return true;
            } else {
                return false;
            }
        } else if (validBlock) {
            view.rewind();
            return true;
        } else {
            return false;
        }
    }

}