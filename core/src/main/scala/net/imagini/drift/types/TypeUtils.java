package net.imagini.drift.types;

import net.imagini.drift.utils.ByteUtils;
import net.imagini.drift.utils.View;

public class TypeUtils {

    final public static int compare(View left, View right, DriftType t) {
        int len = t.getLen();
        return ByteUtils.compare(
            left.array, left.offset + ((len == -1) ?  4 : 0), (len == -1) ? t.sizeOf(left)-4 : len,
            right.array, right.offset + ((len == -1) ?  4 : 0), (len == -1) ? t.sizeOf(right)-4 : len
        );
    }

    final public static boolean contains(View container, View value, DriftType t) {
        int len = t.getLen();
        return ByteUtils.contains(
            container.array, container.offset +((len == -1) ?  4 : 0), (len == -1) ? t.sizeOf(container)-4 : len,
            value.array, value.offset +((len == -1) ?  4 : 0), (len == -1) ? t.sizeOf(value)-4 : len
        );
    }
    final public static boolean equals(View left, View right, DriftType t) {
        return compare(left, right, t) == 0;
    }

    final public static int sizeOf(DriftType type, byte[] src) {
        int len = type.getLen(); 
        if (len == -1) {
            return ByteUtils.asIntValue(src) + 4;
        } else {
            return len;
        }
    }


}
