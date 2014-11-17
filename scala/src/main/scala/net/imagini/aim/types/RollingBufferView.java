package net.imagini.aim.types;

import java.io.IOException;
import java.io.InputStream;

import net.imagini.aim.cluster.StreamUtils;
import net.imagini.aim.utils.ByteUtils;
import net.imagini.aim.utils.View;

/**
 * WritePos=9 Size=33, Bof=33, Ofs = 33, Limit=33
 * =========W-----------------------SBOL AVAILABLE
 * 
 * Ofs=0 Limit=3 WritePos Size=33, Bof=33 O≠≠L=============W---------------SB
 * V≠≠≠AVAILABLE====UNUSED----------
 * 
 * Ofs=4 Limit=7 WritePos Size=33, Bof=33 ----O≠≠L=============W-----------SB
 * ----V AVAILABLE UNUSED
 * 
 * WritePos OfsLimit Bof Size=33 ======W----------O≠≠L========B...S
 * =LE===UNUSED-----V≠≠≠AVAILAB=
 * 
 */
public class RollingBufferView extends View {

    public int writePos;
    public int bof;
    private int eof;
    final public AimType aimType;
    final public AimDataType dataType;

    private static int HACK = 8000000;
    public RollingBufferView(int size, AimType aimType) {
        super(new byte[HACK], 0, -1, HACK); //FIXME use size
        writePos = 0;
        bof = HACK; //FIXME use size
        eof = -1;
        this.aimType = aimType;
        this.dataType = aimType.getDataType();
    }

    @Override
    public boolean eof() {
        return limit + 1 == eof || offset == eof;
    }

    public void markEof() {
        this.eof = writePos;
    }

    public void keep() {
        int len = dataType.getLen();
        if (len == -1) {
            len = ByteUtils.asIntValue(array, writePos) + 4;
        }
        // synchronized (writeLock) {
        writePos += len;
        // writeLock.notify();
        // }
    }

    public View select(InputStream src) throws IOException,
            InterruptedException {
        int len = dataType.getLen();
        if (len == -1) {
            len = StreamUtils.readInt(src);
            writePos = allocate(4 + len);
            ByteUtils.putIntValue(len, array, writePos);
            StreamUtils.read(src, array, writePos + 4, len);
            return new View(array, writePos, writePos + len + 4, size);
        } else {
            writePos = allocate(len);
            StreamUtils.read(src, array, writePos, len);
            return new View(array, writePos, writePos + len, size);
        }
    }

    private int allocate(int bytesToAllocate) throws InterruptedException {
        if (array.length < bytesToAllocate) {
            throw new IllegalArgumentException(bytesToAllocate
                    + " cannot be allocate in a rolling buffer of size " + size);
        }
        return writePos;
        // do {
        // if (writePos < offset) {
        // if (offset - writePos + 1 >= bytesToAllocate) {
        // return writePos;
        // }
        // } else {
        // if (size - writePos >= bytesToAllocate) {
        // bof = size;
        // return writePos;
        // } else if (offset >= bytesToAllocate) {
        // bof = writePos;
        // return 0;
        // }
        // }
        // // synchronized (readLock) {
        // // readLock.wait();
        // // }
        // } while (true);

    }

    public boolean next() throws InterruptedException {
        int len = dataType.getLen();
        if (len == -1) {
            if (locate(4)) {
                len = ByteUtils.asIntValue(array, offset);
                if (locate(len)) {
                    offset -= 4;
                    return true;
                }
            }
        } else {
            if (locate(len)) {
                return true;
            }
        }
        return false;
    }

    private boolean locate(int bytesToRead) throws InterruptedException {
        do
            if (eof()) {
                return false;
            } else if (limit + bytesToRead < writePos) {
                offset = limit + 1;
                limit = offset + bytesToRead - 1;
                return true;
            } else {
                Thread.sleep(10);
            }
        while (true);
    }

}
