package net.imagini.aim.types;

import java.io.IOException;
import java.nio.ByteBuffer;

import net.imagini.aim.utils.ByteUtils;
import net.imagini.aim.utils.View;

public class TypeUtils {

    final public static int compare(View left, View right, AimDataType t) {
        int len = t.getLen();
        return ByteUtils.compare(
            left.array, left.offset + ((len == -1) ?  4 : 0), (len == -1) ? t.sizeOf(left)-4 : len,
            right.array, right.offset + ((len == -1) ?  4 : 0), (len == -1) ? t.sizeOf(right)-4 : len
        );
    }

    final public static boolean contains(View container, View value, AimDataType t) {
        int len = t.getLen();
        return ByteUtils.contains(
            container.array, container.offset +((len == -1) ?  4 : 0), (len == -1) ? t.sizeOf(container)-4 : len,
            value.array, value.offset +((len == -1) ?  4 : 0), (len == -1) ? t.sizeOf(value)-4 : len
        );
    }
    final public static boolean equals(View left, View right, AimDataType t) {
        return compare(left, right, t) == 0;
    }

    final static public int copy(String unparsed, AimType type, ByteBuffer dest)
            throws IOException {
        return copy(type.convert(unparsed), 0, type.getDataType(), dest);
    }

    final public static int sizeOf(AimDataType type, byte[] src) {
        int len = type.getLen(); 
        if (len == -1) {
            return ByteUtils.asIntValue(src) + 4;
        } else {
            return len;
        }
    }

    final public static int sizeOf(AimDataType type, ByteBuffer src) {
        int len = type.getLen(); 
        if (len == -1) {
            return ByteUtils.asIntValue(src) + 4;
        } else {
            return len;
        }
    }

    final public static long copy(ByteBuffer src, AimDataType dataType, ByteBuffer dest) {
        int head = 0;
        int size = dataType.getLen();
        if (size == -1) {
            size = src.getInt();
            dest.putInt(size);
            head = 4;
        }
        int o = dest.arrayOffset();
        o += dest.position();
        src.get(dest.array(), o, size);
        dest.position(dest.position() + size);
        return head + size;
    }

    final static public int copy(byte[] src, int srcOffset, AimDataType type, ByteBuffer dest)
            throws IOException {
        int size = type.getLen();
        if (size == -1) {
            size = ByteUtils.asIntValue(src, srcOffset) + 4;
        }
        dest.put(src, srcOffset, size);
        return size;
    }


}
