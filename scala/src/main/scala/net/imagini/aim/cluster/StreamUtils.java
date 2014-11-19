package net.imagini.aim.cluster;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import net.imagini.aim.types.AimDataType;
import net.imagini.aim.utils.ByteUtils;
import net.imagini.aim.utils.View;

public class StreamUtils {

    static public long copy(InputStream in, OutputStream out) throws IOException {
        long count = 0L;
        int n = 0;
        byte[] buffer = new byte[65535];
        do {
            n = in.read(buffer);
            if (-1 != n) {
                out.write(buffer, 0, n);
                count += n;
            }
        } while (-1 != n);
        return count;
    }

    public static int write(ByteBuffer value, OutputStream out)
            throws IOException {
        // System.err.println("WRITE TYPE FROM BUFFER " + type + " " +
        // type.asString(value));
        byte[] array = value.array();
        int offset = value.position();
        int size = value.limit() - offset;
        out.write(array, offset, size);
        return size;
    }

    public static int write(AimDataType type, ByteBuffer value, OutputStream out)
            throws IOException {
        // System.err.println("WRITE TYPE FROM BUFFER " + type + " " +
        // type.asString(value));
        byte[] array = value.array();
        int offset = value.position();
        int size = type.getLen();
        if (size == -1) {
            size = ByteUtils.asIntValue(array, offset) + 4;
        }
        out.write(array, offset, size);
        return size;
    }

    public static int write(AimDataType type, byte[] value, OutputStream out)
            throws IOException {
        int size = type.getLen();
        if (size == -1) {
            size = ByteUtils.asIntValue(value) + 4;
        }
        // System.err.println("WRITE TYPE FROM ARRAY[" + size + "] " + type +
        // " " + type.convert(value));
        out.write(value, 0, size);
        return size;
    }

    static public void writeInt(OutputStream out, int v) throws IOException {
        // System.err.println("WRITE INT " + v);
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write((v >>> 0) & 0xFF);
    }

    static public int readInt(InputStream in) throws IOException {
        int val;
        val = ((((in.read() & 0xff)) << 24) + (((in.read() & 0xff)) << 16)
                + (((in.read() & 0xff)) << 8) + (((in.read() & 0xff)) << 0));
        if (val < 0) {
            // System.err.println("READ INT EOF");
            throw new EOFException();
        } else {
            // System.err.println("READ INT " + val);
            return val;
        }

    }

    static public int readSize(InputStream in, AimDataType type)
            throws IOException {
        int size = type.getLen();
        if (size == -1) {
            return readInt(in);
        } else {
            return size;
        }
    }

    static public byte[] read(InputStream in, AimDataType type)
            throws IOException {
        int size = readSize(in, type);
        byte[] result;
        if (type.getLen() == -1) {
            result = new byte[size + 4];
            ByteUtils.putIntValue(size, result, 0);
            read(in, result, 4, size);
        } else {
            result = new byte[size];
            read(in, result, 0, size);
        }
        // System.err.println("READ TYPE AS NEW byte[] " + type + " " +
        // type.convert(result));
        return result;
    }

    static public int read(InputStream in, AimDataType type, ByteBuffer buf)
            throws IOException {
        // System.err.println("READ TYPE INTO Buffer " + type);
        int size = type.getLen();
        if (size == -1) {
            size = StreamUtils.readInt(in);
            buf.putInt(size);
        }
        read(in, buf, size);
        return size;
    }

    static public int read(InputStream in, AimDataType type, View view)
            throws IOException {
        // System.err.println("READ TYPE INTO Buffer " + type);
        int size = type.getLen();
        if (size == -1) {
            size = StreamUtils.readInt(in);
            ByteUtils.putIntValue(size, view.array, view.offset);
            view.offset+=4;
            read(in, view.array, view.offset, size);
            view.offset+=size;
            return size + 4;
        } else {
            read(in, view.array, view.offset, size);
            view.offset+=size;
            return size;
        }
    }
    static public long skip(InputStream in, AimDataType type)
            throws IOException {
        // System.err.println("SKIP TYPE " + type);
        int skipLen = readSize(in, type);
        long totalSkipped = 0;
        while (totalSkipped < skipLen) {
            long skipped = in.skip(skipLen - totalSkipped);
            totalSkipped += skipped;
        }
        return totalSkipped;
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
            int read = in.read(buf.array(), buf.position(),
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
