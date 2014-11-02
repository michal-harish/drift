package net.imagini.aim.utils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.commons.io.EndianUtils;

/**
 * 1. byte array utils 2. inputstream utils 3. ByteBuffer utils
 */
public class ByteUtils {

    /**
     * We need to use BIG_ENDIAN because the pro(s) are favourable to our
     * usecase, e.g. lot of streaming filtering.
     */
    final public static ByteOrder ENDIAN = ByteOrder.BIG_ENDIAN;

    public static int compareUnisgned(byte a, byte b) {
        return (a & 0xFF)  - (b & 0xFF);
    }

    /**
     * byte array utils
     */

    static public int asIntValue(byte[] value) {
        return asIntValue(value, 0);
    }

    static public int asIntValue(byte[] value, int offset) {
        if (ENDIAN.equals(ByteOrder.LITTLE_ENDIAN)) {
            return EndianUtils.readSwappedInteger(value, 0);
        } else {
            return ((((int) value[offset + 0]) << 24)
                    + (((int) value[offset + 1] & 0xff) << 16)
                    + (((int) value[offset + 2] & 0xff) << 8) + (((int) value[offset + 3] & 0xff) << 0));
        }
    }

    static public long asLongValue(byte[] value, int o) {
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
     * nio.ByteBuffer utils
     */
    static public ByteBuffer wrap(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value);
        bb.order(ENDIAN);
        return bb;
    }
    static public int asIntValue(final ByteBuffer value) {
        int offset = value.position();
        if (ENDIAN.equals(ByteOrder.LITTLE_ENDIAN)) {
            return (
                    (((int) value.get(offset + 3)) << 24)
                    + (((int) value.get(offset + 2) & 0xff) << 16)
                    + (((int) value.get(offset + 1) & 0xff) << 8) 
                    + (((int) value.get(offset + 0) & 0xff) << 0)
                    );
        } else {
            return (
                    (((int) value.get(offset + 0)) << 24)
                    + (((int) value.get(offset + 1) & 0xff) << 16)
                    + (((int) value.get(offset + 2) & 0xff) << 8) 
                    + (((int) value.get(offset + 3) & 0xff) << 0)
                    );
        }
    }
    static public long asLongValue(final ByteBuffer value) {
        return asLongValue(value, 0);
    }
    static public long asLongValue(final ByteBuffer value, final int ofs) {
        int offset = value.position() + ofs;
        if (ENDIAN.equals(ByteOrder.LITTLE_ENDIAN)) {
            return (
                    (((long) value.get(offset + 7) & 0xff) << 56)
                    + (((long) value.get(offset + 6) & 0xff) << 48)
                    + (((long) value.get(offset + 5) & 0xff) << 40)
                    + (((long) value.get(offset + 4) & 0xff) << 32)
                    + (((long) value.get(offset + 3) & 0xff) << 24)
                    + (((long) value.get(offset + 2) & 0xff) << 16)
                    + (((long) value.get(offset + 1) & 0xff) << 8) 
                    + (((long) value.get(offset + 0) & 0xff) << 0)
                    );
        } else {
            return (
                    (((long) value.get(offset + 0) & 0xff) << 56)
                    + (((long) value.get(offset + 1) & 0xff) << 48)
                    + (((long) value.get(offset + 2) & 0xff) << 40)
                    + (((long) value.get(offset + 3) & 0xff) << 32)
                    + (((long) value.get(offset + 4) & 0xff) << 24)
                    + (((long) value.get(offset + 5) & 0xff) << 16)
                    + (((long) value.get(offset + 6) & 0xff) << 8) 
                    + (((long) value.get(offset + 7) & 0xff) << 0)
                    );
        }
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

}
