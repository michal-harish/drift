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

        int ni;
        int i = left.position();
        int nj;
        int j = right.position();
        int n;
        int k = 0;
        if (type.equals(Aim.STRING)) {
            ni = ByteUtils.asIntValue(left) + 4;
            i += 4;
            nj = ByteUtils.asIntValue(right) + 4;
            j += 4;
            n = Math.min(ni, nj);
            k += 4;
        } else {
            n = ni = nj = type.getDataType().getSize();
        }
        if (ni == nj) {
            for (; k < n; k++, i++, j++) {
                int cmp = ByteUtils.compareUnisgned(left.get(i), right.get(j));
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
        int ni;
        int i = container.position();
        int nj;
        int j = 0;
        if (aimType.getDataType().equals(Aim.STRING)) {
            ni = ByteUtils.asIntValue(container) + 4;
            i += 4;
            nj = ByteUtils.asIntValue(value) + 4;
            j += 4;
        } else {
            ni = nj = aimType.getDataType().getSize();
        }
        if (nj > ni) {
            return false;
        } else {
            ni += container.position();
            int v = j;
            for (; i < ni; i++) {
                byte b = container.get(i);
                if (value.get(v) != b) {
                    v = j;
                } else if (++v == value.limit()) {
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
        // FIXME array buffer specific code should be done with slice instead
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
            size = ByteUtils.getIntValue(src);
            dest.put(src, 0, size + 4);
            return size + 4;
        } else {
            size = type.getSize();
        }
        dest.put(src, 0, size);
        return size;
    }
}
