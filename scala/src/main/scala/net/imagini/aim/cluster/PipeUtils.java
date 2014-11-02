package net.imagini.aim.cluster;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.commons.io.EndianUtils;

import net.imagini.aim.types.Aim;
import net.imagini.aim.types.AimDataType;
import net.imagini.aim.types.AimTypeBYTEARRAY;
import net.imagini.aim.utils.ByteUtils;

public class PipeUtils {

    public static int write(AimDataType type, ByteBuffer value, OutputStream out)
            throws IOException {
        byte[] array = value.array();
        int offset = value.arrayOffset() + value.position();
        int size = 0;
        if (type.equals(Aim.STRING)) {
            size = ByteUtils.getIntValue(array, offset) + 4;
        } else {
            size = type.getSize();
        }
        out.write(array, offset, size);
        return size;
    }

    public static int write(AimDataType type, byte[] value, OutputStream out)
            throws IOException {
        int size = 0;
        if (type.equals(Aim.STRING)) {
            size = ByteUtils.getIntValue(value);
            out.write(value, 0, size + 4);
            return size + 4;
        } else {
            size = type.getSize();
        }
        out.write(value, 0, size);
        return size;
    }

    static private int readSize(InputStream in, AimDataType type) throws IOException {
        if (type.equals(Aim.STRING)) {
            byte[] b = new byte[4];
            read(in,b,0,4);
            return ByteUtils.getIntValue(b);
        } else if (type instanceof AimTypeBYTEARRAY) {
            return ((AimTypeBYTEARRAY)type).size;
        } else {
            return type.getSize();
        }
    }

    static public byte[] read(InputStream in, AimDataType type) throws IOException {
        int size = readSize(in, type);
        byte[] result;
        if (type.equals(Aim.STRING)) {
            result = new byte[size + 4];
            ByteUtils.putIntValue(size,result,0);
            read( in, result, 4 , size);
        } else {
            result = new byte[size];
            read( in, result, 0 , size);
        }
        return result;
    }

    static public void skip(InputStream in, AimDataType type) throws IOException {
        int skipLen = readSize(in, type);
        long totalSkipped = 0;
        while (totalSkipped < skipLen) {
            long skipped = in.skip(skipLen-totalSkipped);
            totalSkipped += skipped;
        }
    }

    private static byte[] intBuffer = new byte[4];

    // FIXME sizeOf is non-thread-safe because of intBuffer;
    public static int sizeOf(InputStream in, AimDataType type)
            throws IOException {
        int size;
        if (type.equals(Aim.STRING)) {
            read(in, intBuffer, 0, 4);
            size = ByteUtils.getIntValue(intBuffer);
        } else {
            size = type.getSize();
        }
        return size;
    }

    static public int read(InputStream in, AimDataType type, byte[] buf)
            throws IOException {
        int size;
        int offset = 0;
        if (type.equals(Aim.STRING)) {
            read(in, buf, 0, (offset = 4));
            size = ByteUtils.getIntValue(buf);
        } else {
            size = type.getSize();
        }
        read(in, buf, offset, size);
        return offset + size;
    }

    static public void write(OutputStream out, int v) throws IOException {
        if (ByteUtils.ENDIAN.equals(ByteOrder.LITTLE_ENDIAN)) {
            EndianUtils.writeSwappedInteger(out, v);
        } else {
            out.write((v >>> 24) & 0xFF);
            out.write((v >>> 16) & 0xFF);
            out.write((v >>> 8) & 0xFF);
            out.write((v >>> 0) & 0xFF);
        }
    }

    static public void write(OutputStream out, long v) throws IOException {
        if (ByteUtils.ENDIAN.equals(ByteOrder.LITTLE_ENDIAN)) {
            EndianUtils.writeSwappedLong(out, v);
        } else {
            out.write((byte) (v >>> 56));
            out.write((byte) (v >>> 48));
            out.write((byte) (v >>> 40));
            out.write((byte) (v >>> 32));
            out.write((byte) (v >>> 24));
            out.write((byte) (v >>> 16));
            out.write((byte) (v >>> 8));
            out.write((byte) (v >>> 0));
        }
    }

    static public void read(InputStream in, byte[] buf, int offset, int len)
            throws IOException {
        int totalRead = 0;
        while (totalRead < len) {
            int read = in.read(buf, offset + totalRead, len - totalRead);
            if (read < 0)
                throw new EOFException();
            else
                totalRead += read;
        }
    }

    static public void read(InputStream in, ByteBuffer buf, int len)
            throws IOException {
        int totalRead = 0;
        while (totalRead < len) {
            int read = in.read(buf.array(), buf.arrayOffset() + buf.position(),
                    len - totalRead);
            if (read < 0)
                throw new EOFException();
            else {
                buf.position(buf.position() + read);
                totalRead += read;
            }
        }
    }

}
