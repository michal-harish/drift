package net.imagini.aim.types;

import java.io.IOException;
import java.nio.ByteBuffer;

import net.imagini.aim.utils.ByteUtils;

public class TypeUtils {

    final public static boolean equals(ByteBuffer left, ByteBuffer right, AimType type) {
        return compare(left, right, type) == 0;
    }

    /**
     * Compares the current buffer position if treated as given type with the given value
     * but does not advance
     */
    final public static int compare(ByteBuffer left, ByteBuffer right, AimType type) {
        AimDataType d = type.getDataType();
        byte[] lArray = left.array();
        byte[] rArray = right.array();
        int ni;
        int i = left.position() + left.arrayOffset();
        int nj;
        int j = right.position()+ right.arrayOffset();
        int n;
        int k = 0;
        if (type.equals(Aim.STRING)) {
            ni = ByteUtils.asIntValue(lArray, i) + 4;
            i += 4;
            nj = ByteUtils.asIntValue(rArray, j) + 4;
            j += 4;
            n = Math.min(ni, nj);
            k += 4;
        } else {
            n = ni = nj = d.getSize();
        }
        if (ni == nj) {
            for (; k < n; k++, i++, j++) {
                int cmp = (lArray[i] & 0xFF)  - (rArray[j] & 0xFF);
                if (cmp != 0) {
                    return cmp;
                }
            }
        }
        return ni - nj;
    }

    /**
     * Checks if the current buffer position if treated as given type would contain the given value
     * but does not advance
     */
    final public static boolean contains(ByteBuffer container, ByteBuffer value, AimType aimType) {
        AimDataType d = aimType.getDataType();
        byte[] cArray = container.array();
        byte[] vArray = value.array();
        int vLimit = value.limit() + value.arrayOffset();
        int ni;
        int i = container.position() + container.arrayOffset();
        int nj;
        int j = value.position() + value.arrayOffset();
        if (d.equals(Aim.STRING)) {
            ni = ByteUtils.asIntValue(cArray, i) + 4;
            i += 4;
            nj = ByteUtils.asIntValue(vArray, 0) + 4;
            j += 4;
        } else {
            ni = nj = d.getSize();
        }
        if (nj > ni) {
            return false;
        } else {
            ni += container.position() + container.arrayOffset();
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

    final public static long copy(ByteBuffer src, AimDataType type, ByteBuffer dest) {
        int size;
        int head = 0;
        if (type.equals(Aim.STRING)) {
            size = src.getInt();
            dest.putInt(size);
            head = 4;
        } else {
            size = type.getSize();
        }
        int o = dest.arrayOffset();
        o += dest.position();
        src.get(dest.array(), o, size);
        dest.position(dest.position() + size);
        return head + size;
    }

    final static public int copy(byte[] src, AimDataType type, ByteBuffer dest)
            throws IOException {
        int size = 0;
        if (type.equals(Aim.STRING)) {
            size = ByteUtils.asIntValue(src);
            dest.put(src, 0, size + 4);
            return size + 4;
        } else {
            size = type.getSize();
        }
        dest.put(src, 0, size);
        return size;
    }
}
