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

    public static int parseIntRadix10(byte[] array, int offset, int limit) {
        int result = 0;
        boolean negative = false; 
        if (array[offset] == '-' ) {
            negative = true;
            offset++;
        }
        for(int i = offset; i<= limit; i++) {
            if (array[i] < 48 || array[i] > 57) {
                throw new IllegalArgumentException("Invalid numeric character " +  (char)array[i]);
            }
            result *= 10;
            result += (array[i] - 48) ;
        }
        return negative ? -result : result;
    }

    public static long parseLongRadix10(byte[] array, int offset, int limit) {
        long result = 0;
        for(int i = offset; i<= limit; i++) {
            if (array[i] < 48 || array[i] > 57) {
                throw new IllegalArgumentException("Invalid numeric character " +  (char)array[i]);
            }
            result *= 10;
            result += (array[i] - 48) ;
        }
        return result;
    }


    public static long parseLongRadix16(byte[] array, int offset, int limit) {
        long result = 0;
        for(int i = offset; i<= limit; i++) {
            result *= 16;
            if (array[i] >= 48 && array[i] <= 57) {
                result += (array[i] - 48) ;
            } else if (array[i] >= 'A' && array[i] <= 'F') {
                result += (array[i] - 55) ;
            } else if (array[i] >= 97 && array[i] <= 102) {
                result += (array[i] - 87) ;
            } else {
                throw new IllegalArgumentException("Invalid numeric character " +  (char)array[i]);
            }
        }
        return result;
    }

    public static int copy(byte[] src, int srcOffset, byte[]dest, int destOffset, int len) {
        for(int i = 0; i< len; i++) {
            dest[i+destOffset] = src[i+srcOffset];
        }
        return len;
    }

    static public int asIntValue(byte[] value) {
        return asIntValue(value, 0);
    }

    static public long asLongValue(byte[] value) {
        return asLongValue(value, 0);
    }

    static public int asIntValue(byte[] value, int offset) {
        return (  (((int) value[offset + 0]) << 24)
                + (((int) value[offset + 1] & 0xff) << 16)
                + (((int) value[offset + 2] & 0xff) << 8) 
                + (((int) value[offset + 3] & 0xff) << 0));

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
    final public static boolean equals(byte[] lArray, int leftOffset, int lSize, byte[] rArray, int rightOffset, int rSize) {
        return compare(lArray, leftOffset, lSize, rArray, rightOffset, rSize) == 0;
    }
    final public static int compare(byte[] lArray, int leftOffset, int lSize, byte[] rArray, int rightOffset, int rSize) {
        if (lSize != rSize) {
            return lSize - rSize;
        }
        int i = leftOffset;
        int j = rightOffset;
        int n = lSize -1;
        for (int k = 0; k < n; k++, i++, j++) {
            int cmp = (lArray[i] & 0xFF) - (rArray[j] & 0xFF);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    /**
     * Checks if the current buffer position if treated as given type would
     * contain the given value but does not advance
     */
    final public static boolean contains(byte[] cArray, int cOffset, int cSize, byte[] vArray, int vOffset, int vSize) {
        if (cSize == 0 || vSize == 0 || vSize > cSize) {
            return false;
        }
        int cLimit = cOffset + cSize -1;
        int vLimit = vOffset + vSize -1;
        int v = vOffset;
        for (int c = cOffset; c <= cLimit; c++) {
            if (vArray[v] != cArray[c]) {
                v = vOffset;
                if (c + vSize > cLimit) {
                    return false;
                }
            } else if (++v >= vLimit) {
                return true;
            }
        }
        return false;
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

    public static int crc32(byte[] array, int offset, int size) {
        int crc = 0xFFFF;
        for (int pos = offset; pos < offset + size; pos++) {
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
