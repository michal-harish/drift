package net.imagini.aim.utils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * 1. byte array utils 2. inputstream utils 3. ByteBuffer utils
 * 
 * We need to use BIG_ENDIAN because the pro(s) are favourable to our usecase,
 * e.g. lot of streaming filtering.
 */
public class ByteUtils {

    /**
     * byte array utils
     */

    static public int asIntValue(byte[] value) {
        return asIntValue(value, 0);
    }

    static public int asIntValue(byte[] value, int offset) {
        return ((((int) value[offset + 0]) << 24)
                + (((int) value[offset + 1] & 0xff) << 16)
                + (((int) value[offset + 2] & 0xff) << 8) + (((int) value[offset + 3] & 0xff) << 0));

    }

    static public long asLongValue(byte[] value, int o) {
        return (((long) value[o + 0] << 56)
                + (((long) value[o + 1] & 0xff) << 48)
                + (((long) value[o + 2] & 0xff) << 40)
                + (((long) value[o + 3] & 0xff) << 32)
                + (((long) value[o + 4] & 0xff) << 24)
                + (((long) value[o + 5] & 0xff) << 16)
                + (((long) value[o + 6] & 0xff) << 8) + (((long) value[o + 7] & 0xff) << 0));
    }

    public static void putIntValue(int value, byte[] result, int offset) {
        result[offset + 0] = (byte) ((value >>> 24) & 0xFF);
        result[offset + 1] = (byte) ((value >>> 16) & 0xFF);
        result[offset + 2] = (byte) ((value >>> 8) & 0xFF);
        result[offset + 3] = (byte) ((value >>> 0) & 0xFF);
    }

    public static void putLongValue(long value, byte[] result, int offset) {
        result[offset + 0] = (byte) ((value >>> 56) & 0xFF);
        result[offset + 1] = (byte) ((value >>> 48) & 0xFF);
        result[offset + 2] = (byte) ((value >>> 40) & 0xFF);
        result[offset + 3] = (byte) ((value >>> 32) & 0xFF);
        result[offset + 4] = (byte) ((value >>> 24) & 0xFF);
        result[offset + 5] = (byte) ((value >>> 16) & 0xFF);
        result[offset + 6] = (byte) ((value >>> 8) & 0xFF);
        result[offset + 7] = (byte) ((value >>> 0) & 0xFF);
    }

    /**
     * Compares the current buffer position if treated as given type with the
     * given value but does not advance
     */
    final public static int compare(byte[] lArray, int leftOffset, byte[] rArray, int rightOffset, int len) {
        int ni;
        int i = leftOffset;
        int nj;
        int j = rightOffset;
        int n;
        int k = 0;
        if (len == -1) {
            ni = ByteUtils.asIntValue(lArray, i) + 4;
            i += 4;
            nj = ByteUtils.asIntValue(rArray, j) + 4;
            j += 4;
            n = Math.min(ni, nj);
            k += 4;
        } else {
            n = ni = nj = len;
        }
        if (ni == nj) {
            for (; k < n; k++, i++, j++) {
                int cmp = (lArray[i] & 0xFF) - (rArray[j] & 0xFF);
                if (cmp != 0) {
                    return cmp;
                }
            }
        }
        return ni - nj;
    }

    /**
     * Checks if the current buffer position if treated as given type would
     * contain the given value but does not advance
     */
    final public static boolean contains(byte[] cArray, int cOffset, byte[] vArray, int vOffset, int len) {
        int vLimit;
        int ni;
        int i = cOffset;
        int nj;
        int j = vOffset;
        if (len == -1) {
            vLimit = vArray.length;
            ni = ByteUtils.asIntValue(cArray, i) + 4;
            i += 4;
            nj = ByteUtils.asIntValue(vArray, 0) + 4;
            j += 4;
        } else {
            vLimit = vOffset + len;
            ni = nj = len;
        }
        if (nj > ni) {
            return false;
        } else {
            ni += cOffset;
            int v = j;
            for (; i < ni; i++) {
                if (vArray[v] != cArray[i]) {
                    v = j;
                } else if (++v == vLimit) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * View utils
     */
    static public int asIntValue(final View value) {
        return asIntValue(value.array, value.offset);
    }

    static public long asLongValue(final View value) {
        return asLongValue(value.array, value.offset);
    }
    static public long asLongValue(final View value, final int offset) {
        return asLongValue(value.array, value.offset + offset);
    }

    final public static int compare(View left, View right, int len) {
        return compare(
            left.array, left.offset,
            right.array, right.offset,
            len
        );
    }

    final public static boolean contains(View container, View value, int len) {
        return contains(
            container.array, container.offset,
            value.array, value.offset,
            len
        );
    }
    final public static boolean equals(View left, View right,
            int len) {
        return compare(left, right, len) == 0;
    }

    /**
     * nio.ByteBuffer utils
     */
    static public ByteBuffer wrap(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value);
        bb.order(ByteOrder.BIG_ENDIAN);
        return bb;
    }

    public static ByteBuffer createBuffer(int size) {
        ByteBuffer record = ByteBuffer.allocate(size);
        record.order(ByteOrder.BIG_ENDIAN);
        return record;
    }

    public static ByteBuffer createIntBuffer(int value) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.order(ByteOrder.BIG_ENDIAN);
        bb.putInt(value);
        return bb;
    }

    public static ByteBuffer createLongBuffer(Long value) {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.order(ByteOrder.BIG_ENDIAN);
        bb.putLong(Long.valueOf(value));
        return bb;
    }

    public static ByteBuffer createStringBuffer(String value) {
        byte[] val = value.getBytes();
        ByteBuffer bb = ByteBuffer.allocate(val.length + 4);
        bb.order(ByteOrder.BIG_ENDIAN);
        bb.putInt(value.length());
        bb.put(val);
        return bb;
    }

    static public int asIntValue(final ByteBuffer value) {
        return asIntValue(value.array(), value.position());
    }

    static public long asLongValue(final ByteBuffer value) {
        return asLongValue(value, 0);
    }

    static public long asLongValue(final ByteBuffer value, final int ofs) {
        return asLongValue(value.array(), value.position() + ofs);
    }

    final public static boolean equals(ByteBuffer left, ByteBuffer right,
            int len) {
        return compare(left, right, len) == 0;
    }

    /**
     * Compares the current buffer position if treated as given type with the
     * given value but does not advance
     */
    final public static int compare(ByteBuffer left, ByteBuffer right, int len) {
        return compare(
            left.array(), left.position(),
            right.array(), right.position(),
            len
        );
    }

    final public static boolean contains(ByteBuffer container, ByteBuffer value, int len) {
        return contains(
            container.array(), container.position(),
            value.array(), value.position(),
            len
        );
    }

    public static int crc32(byte[] array, int offset, int size) {
        int crc = 0xFFFF;
        for (int pos = offset; pos <= offset + size; pos++) {
            crc ^= (int) array[pos];
            for (int i = 8; i != 0; i--) {
                if ((crc & 0x0001) != 0) {
                    crc >>= 1;
                    crc ^= 0xA001;
                } else {
                    crc >>= 1;
                }
            }
        }
        return crc;
    }

    public static int sum(byte[] array, int offset, int size) {
        int sum = 0;
        for (int i = offset; i< offset + size; i++) sum ^=  array[i];
        return sum;
    }
}
