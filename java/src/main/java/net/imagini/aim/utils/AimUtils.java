package net.imagini.aim.utils;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import net.imagini.aim.types.Aim;
import net.imagini.aim.types.AimDataType;
import org.apache.commons.io.EndianUtils;

/**
 * 1. byte array utils 2. inputstream utils 3. ByteBuffer utils
 */
public class AimUtils {

    /**
     * We need to use BIG_ENDIAN because the pro(s) are favourable to our
     * usecase, e.g. lot of streaming filtering.
     */
    final public static ByteOrder ENDIAN = ByteOrder.BIG_ENDIAN;

    /**
     * 1. byte[] utils
     */

    static public int getIntValue(byte[] value) {
        return getIntValue(value, 0);
    }

    static public int getIntValue(byte[] value, int offset) {
        if (ENDIAN.equals(ByteOrder.LITTLE_ENDIAN)) {
            return EndianUtils.readSwappedInteger(value, 0);
        } else {
            return ((((int) value[offset + 0]) << 24)
                    + (((int) value[offset + 1] & 0xff) << 16)
                    + (((int) value[offset + 2] & 0xff) << 8) + (((int) value[offset + 3] & 0xff) << 0));
        }
    }

    static public long getLongValue(byte[] value, int o) {
        if (ENDIAN.equals(ByteOrder.LITTLE_ENDIAN)) {
            return EndianUtils.readSwappedLong(value, o);
        } else {
            return (((long) value[o + 0] << 56)
                    + (((long) value[o + 1] & 0xff) << 48)
                    + (((long) value[o + 2] & 0xff) << 40)
                    + (((long) value[o + 3] & 0xff) << 32)
                    + (((long) value[o + 4] & 0xff) << 24)
                    + (((long) value[o + 5] & 0xff) << 16)
                    + (((long) value[o + 6] & 0xff) << 8) + (((long) value[o + 7] & 0xff) << 0));
        }
    }

    public static void putIntValue(int value, byte[] result, int offset) {
        if (ENDIAN.equals(ByteOrder.LITTLE_ENDIAN)) {
            EndianUtils.writeSwappedInteger(result, offset, value);
        } else {
            result[offset + 0] = (byte) ((value >>> 24) & 0xFF);
            result[offset + 1] = (byte) ((value >>> 16) & 0xFF);
            result[offset + 2] = (byte) ((value >>> 8) & 0xFF);
            result[offset + 3] = (byte) ((value >>> 0) & 0xFF);
        }
    }

    /**
     * 2. Stream utils
     */

    static public void write(OutputStream out, int v) throws IOException {
        if (ENDIAN.equals(ByteOrder.LITTLE_ENDIAN)) {
            EndianUtils.writeSwappedInteger(out, v);
        } else {
            out.write((v >>> 24) & 0xFF);
            out.write((v >>> 16) & 0xFF);
            out.write((v >>> 8) & 0xFF);
            out.write((v >>> 0) & 0xFF);
        }
    }

    static public void write(OutputStream out, long v) throws IOException {
        if (ENDIAN.equals(ByteOrder.LITTLE_ENDIAN)) {
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

    public static int write(AimDataType type, byte[] value, OutputStream out)
            throws IOException {
        int size = 0;
        if (type.equals(Aim.STRING)) {
            size = getIntValue(value);
            out.write(value, 0, size + 4);
            return size + 4;
        } else {
            size = type.getSize();
        }
        out.write(value, 0, size);
        return size;
    }

    public static long skip(InputStream in, AimDataType type)
            throws IOException {
        int size;
        if (type.equals(Aim.STRING)) {
            byte[] buf = new byte[4];
            read(in, buf, 0, 4);
            size = getIntValue(buf);
        } else {
            size = type.getSize();
        }
        return in.skip(size);
    }

    private static byte[] intBuffer = new byte[4];

    // FIXME sizeOf is non-thread-safe because of intBuffer;
    public static int sizeOf(InputStream in, AimDataType type)
            throws IOException {
        int size;
        if (type.equals(Aim.STRING)) {
            read(in, intBuffer, 0, 4);
            size = getIntValue(intBuffer);
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
            size = getIntValue(buf);
        } else {
            size = type.getSize();
        }
        read(in, buf, offset, size);
        return offset + size;
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

    /**
     * 3. nio.ByteBuffer utils
     */
    static public int getIntValue(ByteBuffer value) {
        int offset = value.position();
        if (ENDIAN.equals(ByteOrder.LITTLE_ENDIAN)) {
            return ((((int) value.get(offset + 3)) << 24)
                    + (((int) value.get(offset + 2) & 0xff) << 16)
                    + (((int) value.get(offset + 1) & 0xff) << 8) + (((int) value
                    .get(offset + 0) & 0xff) << 0));
        } else {
            return ((((int) value.get(offset + 0)) << 24)
                    + (((int) value.get(offset + 1) & 0xff) << 16)
                    + (((int) value.get(offset + 2) & 0xff) << 8) + (((int) value
                    .get(offset + 3) & 0xff) << 0));
        }
    }

    public static long copy(ByteBuffer src, AimDataType type, ByteBuffer dest) {
        int size;
        int head = 0;
        if (type.equals(Aim.STRING)) {
            size = src.getInt();
            dest.putInt(size);
            head = 4;
        } else {
            size = type.getSize();
        }
        // FIXME array buffer specific code should be done with slice instead
        int o = dest.arrayOffset();
        o += dest.position();
        src.get(dest.array(), o, size);
        dest.position(dest.position() + size);
        return head + size;
    }

    public static ByteBuffer createBuffer(int size) {
        ByteBuffer record = ByteBuffer.allocate(size);
        record.order(ENDIAN);
        return record;
    }

    public static ByteBuffer createIntBuffer(int value) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.order(ENDIAN);
        bb.putInt(value);
        return bb;
    }

    public static ByteBuffer createLongBuffer(Long value) {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.order(ENDIAN);
        bb.putLong(Long.valueOf(value));
        return bb;
    }

    public static ByteBuffer createStringBuffer(String value) {
        byte[] val = value.getBytes();
        ByteBuffer bb = ByteBuffer.allocate(val.length + 4);
        bb.order(ENDIAN);
        bb.putInt(value.length());
        bb.put(val);
        return bb;
    }

    public static long skip(ByteBuffer in, AimDataType type) {
        int size = sizeOf(in, type);
        in.position(in.position() + size);
        return size;
    }

    public static int sizeOf(ByteBuffer in, AimDataType type) {
        int size;
        if (type.equals(Aim.STRING)) {
            size = getIntValue(in) + 4;
        } else {
            size = type.getSize();
        }
        return size;
    }

    static public int write(AimDataType type, byte[] value, ByteBuffer out)
            throws IOException {
        int size = 0;
        if (type.equals(Aim.STRING)) {
            size = getIntValue(value);
            out.put(value, 0, size + 4);
            return size + 4;
        } else {
            size = type.getSize();
        }
        out.put(value, 0, size);
        return size;
    }

}
