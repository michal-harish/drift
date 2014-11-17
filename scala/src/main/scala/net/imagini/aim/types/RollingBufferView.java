package net.imagini.aim.types;

import java.io.IOException;
import java.io.InputStream;

import net.imagini.aim.cluster.StreamUtils;
import net.imagini.aim.utils.ByteUtils;
import net.imagini.aim.utils.View;

/**
 *           WritePos=9              Size=33, Bof=33, Ofs = 33, Limit=33
 *  =========W-----------------------SBOL
 *  AVAILABLE
 *
 *  Ofs=0 Limit=3    WritePos        Size=33, Bof=33
 *  O≠≠L=============W---------------SB
 *  V≠≠≠AVAILABLE====UNUSED----------
 *
 *      Ofs=4 Limit=7    WritePos    Size=33, Bof=33
 *  ----O≠≠L=============W-----------SB
 *  ----V   AVAILABLE    UNUSED
 *
 *        WritePos   OfsLimit    Bof Size=33
 *  ======W----------O≠≠L========B...S
 *  =LE===UNUSED-----V≠≠≠AVAILAB=
 *
 */
public class RollingBufferView extends View {

    public int writePos;
    public int bof;
    final public AimType aimType;
    final public AimDataType dataType;

    public RollingBufferView(int size, AimType aimType) {
        super(new byte[size], size, size, size);
        writePos =  0;
        bof = size;
        this.aimType = aimType;
        this.dataType = aimType.getDataType();
    }

    @Override public boolean eof() {
        return false;
    }

    public void keep() {
        writePos = limit + 1;
    }

    public void select(InputStream src) throws IOException, InterruptedException {
        int len = dataType.getLen();
        if (len == -1) {
            len = StreamUtils.readInt(src);
            offset = writePos = allocate(4 + len);
            ByteUtils.putIntValue(len, array, writePos);
            StreamUtils.read(src, array, writePos + 4, len);
            this.limit = offset + len + 4;
        } else {
            offset = writePos = allocate(len);
            StreamUtils.read(src, array, writePos, len);
            this.limit = offset + len;
        }
    }


    synchronized public void put(InputStream src) throws IOException, InterruptedException {
        if (dataType.getLen() == -1) {
            int len = StreamUtils.readInt(src);
            writePos = allocate(4 + len);
            ByteUtils.putIntValue(len, array, writePos);
            StreamUtils.read(src, array, writePos + 4, len);
            writePos += len + 4;
        } else {
            writePos = allocate(dataType.getLen());
            StreamUtils.read(src, array, writePos, dataType.getLen());
            writePos += dataType.getLen();
        }
    }

    private int allocate(int bytesToAllocate) throws InterruptedException {
        if (array.length < bytesToAllocate) {
            throw new IllegalArgumentException(bytesToAllocate
                    + " cannot be allocate in a rolling buffer of size " + size);
        }
        do {
            if (writePos <= offset) {
                if (offset - writePos+1 >= bytesToAllocate)  return writePos;
            } else if (bof - writePos >= bytesToAllocate) {
                return writePos;
            } else if (offset >= bytesToAllocate) {
                bof = writePos;
                return 0;
            }
            synchronized(this) {
                wait();
            }
        } while (true);

    }

    public boolean next() throws InterruptedException {
        int len = dataType.getLen(); 
        if (len == -1) {
            locate(4);
            len = ByteUtils.asIntValue(array, offset);
            locate(len);
            offset -= 4;
            return true;
        } else {
            locate(len);
            return true;
        }
    }

    private boolean locate(int bytesToRead) throws InterruptedException {
        do synchronized (this) {
            if (limit+1 >= bof && writePos >= bytesToRead) {
                offset = 0;
                limit = bytesToRead - 1;
                bof = size;
                notify();
                return true;
            } else if (limit < writePos && writePos - limit + 1 >= bytesToRead) {
                offset = limit + 1;
                limit = offset + bytesToRead - 1;
                notify();
                return true;
            } else if (writePos <= offset && bof - limit + 1 >= bytesToRead) {
                offset = limit + 1;
                limit = offset + bytesToRead - 1;
                notify();
                return true;
            }
        } while(true);
    }

}
