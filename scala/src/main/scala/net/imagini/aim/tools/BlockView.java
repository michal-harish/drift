package net.imagini.aim.tools;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import net.imagini.aim.types.AimDataType;
import net.imagini.aim.utils.BlockStorage;
import net.imagini.aim.utils.ByteUtils;
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
    private InputStream blockInputStream;
    private boolean eof = false;
    private int currentBlock = -1;
    private int currentSkip = -1;
    public int count;

    public BlockView(BlockStorage blockStorage, AimDataType aimDataType) throws IOException {
        this.blockStorage = blockStorage;
        this.dataType = aimDataType;
        this.eof = blockStorage.numBlocks() == 0;
        //FIXME hard-coded rolling buffer !
        this.size = 65535;
        this.array = new byte[size];
        this.offset = -1;
        if (!eof) {
            this.blockInputStream = blockStorage.openInputStream(0);
            currentBlock = 0;
            this.eof = skip() == -1;
        }
    }

    @Override public boolean eof() {
        return eof;
    }

    @Override public int skip() throws IOException {
        while(!eof) try {
            int skipped = 0;
            if (offset == -1) {
                offset = 0;
            } else {
                skipped = currentSkip;
            }
            offset += skipped;
            int mark = offset;
            int len = dataType.getLen();
            currentSkip = 0;
            if (len == -1) {
                len = StreamUtils.readInt(blockInputStream);
                if (offset + 4 + len > array.length) {
                    offset = mark = 0;
                }
                ByteUtils.putIntValue(len, array, offset);
                offset +=4;
                currentSkip = 4 + len;
            } else {
                if (offset + len > array.length) {
                    offset = mark =  0;
                }
                currentSkip = len;
            }
            StreamUtils.read(blockInputStream, array, offset, len);
            offset = mark;
            return skipped;

        } catch (EOFException e) {
            blockInputStream.close();
            currentBlock++;
            if (currentBlock < blockStorage.numBlocks()) {
                this.blockInputStream = blockStorage.openInputStream(currentBlock);
                //TODO ensure closing of the last open input stream from the segment scanner 
            } else {
                eof = true;
            }
        }
        return -1;
    }

}