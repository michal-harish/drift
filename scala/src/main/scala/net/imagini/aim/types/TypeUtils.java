package net.imagini.aim.types;

import java.io.IOException;
import java.nio.ByteBuffer;

import net.imagini.aim.utils.ByteUtils;

public class TypeUtils {


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

    final static public int copy(byte[] src, AimDataType type, ByteBuffer dest)
            throws IOException {
        int size = type.getLen();
        if (size == -1) {
            size = ByteUtils.asIntValue(src);
            dest.put(src, 0, size + 4);
            return size + 4;
        } 
        dest.put(src, 0, size);
        return size;
    }
}
