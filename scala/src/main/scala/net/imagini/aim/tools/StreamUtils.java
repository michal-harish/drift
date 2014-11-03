package net.imagini.aim.tools;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import net.imagini.aim.types.Aim;
import net.imagini.aim.types.AimDataType;
import net.imagini.aim.types.AimTypeBYTEARRAY;
import net.imagini.aim.utils.ByteUtils;

public class StreamUtils {

    public static int write(AimDataType type, ByteBuffer value, OutputStream out)
            throws IOException {
//        System.err.println("WRITE TYPE FROM BUFFER " + type + " " + type.asString(value));
        byte[] array = value.array();
        int offset = value.arrayOffset() + value.position();
        int size = 0;
        if (type.equals(Aim.STRING)) {
            size = ByteUtils.asIntValue(array, offset) + Aim.STRING.size;
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
            size = ByteUtils.asIntValue(value) + Aim.STRING.size;
        } else {
            size = type.getSize();
        }
//        System.err.println("WRITE TYPE FROM ARRAY["+size+"] " + type + " " + type.convert(value));
        out.write(value, 0, size);
        return size;
    }

    static public void writeInt(OutputStream out, int v) throws IOException {
//        System.err.println("WRITE INT " + v);

        if (ByteUtils.ENDIAN.equals(ByteOrder.LITTLE_ENDIAN)) {
            out.write((v >>> 0) & 0xFF);
            out.write((v >>> 8) & 0xFF);
            out.write((v >>> 16) & 0xFF);
            out.write((v >>> 24) & 0xFF);
        } else {
            out.write((v >>> 24) & 0xFF);
            out.write((v >>> 16) & 0xFF);
            out.write((v >>> 8) & 0xFF);
            out.write((v >>> 0) & 0xFF);
        }
    }

    static public int readInt(InputStream in) throws IOException {
        int val;
        if (ByteUtils.ENDIAN.equals(ByteOrder.LITTLE_ENDIAN)) {
            val = ((((in.read() & 0xff)) << 0) + (((in.read() & 0xff)) << 8)
                    + (((in.read() & 0xff)) << 16) + (((in.read() & 0xff)) << 24));
        } else {
            val = ((((in.read() & 0xff)) << 24) + (((in.read() & 0xff)) << 16)
                    + (((in.read() & 0xff)) << 8) + (((in.read() & 0xff)) << 0));
        }
        if (val < 0) {
//            System.err.println("READ INT EOF");
            throw new EOFException();
        } else {
//            System.err.println("READ INT " + val);
            return val;
        }

    }

    static private int readSize(InputStream in, AimDataType type)
            throws IOException {
        if (type.equals(Aim.STRING)) {
            return readInt(in);
        } else if (type instanceof AimTypeBYTEARRAY) {
            return ((AimTypeBYTEARRAY) type).size;
        } else {
            return type.getSize();
        }
    }

    static public byte[] read(InputStream in, AimDataType type)
            throws IOException {
        int size = readSize(in, type);
        byte[] result;
        if (type.equals(Aim.STRING)) {
            result = new byte[size + 4];
            ByteUtils.putIntValue(size, result, 0);
            read(in, result, 4, size);
        } else {
            result = new byte[size];
            read(in, result, 0, size);
        }
//        System.err.println("READ TYPE AS NEW byte[] " + type + " " + type.convert(result));
        return result;
    }

    static public int read(InputStream in, AimDataType type, ByteBuffer buf)
            throws IOException {
//        System.err.println("READ TYPE INTO Buffer " + type);
        int size;
        if (type.equals(Aim.STRING)) {
            size = StreamUtils.readInt(in);
            buf.putInt(size);
        } else {
            size = type.getSize();
        }
        read(in, buf, size);
        return size;
    }

    static public long skip(InputStream in, AimDataType type)
            throws IOException {
//        System.err.println("SKIP TYPE " + type);
        int skipLen = readSize(in, type);
        long totalSkipped = 0;
        while (totalSkipped < skipLen) {
            long skipped = in.skip(skipLen - totalSkipped);
            totalSkipped += skipped;
        }
        return totalSkipped;
    }

    static private void read(InputStream in, byte[] buf, int offset, int len)
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

    static private void read(InputStream in, ByteBuffer buf, int len)
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