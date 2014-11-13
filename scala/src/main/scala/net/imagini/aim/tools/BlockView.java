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
    public int count;

    public BlockView(BlockStorage blockStorage, AimDataType aimDataType) throws IOException {
        this.blockStorage = blockStorage;
        this.dataType = aimDataType;
        this.eof = blockStorage.numBlocks() == 0;
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
            int len = dataType.getLen();
            int o = 0;
            if (len == -1) {
                len = StreamUtils.readInt(blockInputStream);
                if (this.array == null || len+4 > this.array.length) this.array = new byte[len+4];
                ByteUtils.putIntValue(len, this.array, 0);
                o +=4;
            } else {
                if (this.array == null || len+4 > this.array.length) this.array = new byte[len+4];
            }

            StreamUtils.read(blockInputStream, this.array, o, len);
            this.offset = 0;
            this.size = len;
            return len;
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