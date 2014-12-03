package net.imagini.drift.types;

import net.imagini.drift.utils.ByteUtils;
import net.imagini.drift.utils.View;

public class AimTypeBYTE extends AimType {

    @Override
    public int getLen() {
        return 1;
    }

    @Override
    public int parse(View value, byte[] dest, int destOffset) {
        int result = ByteUtils.parseIntRadix10(value.array, value.offset, value.limit);
        if (result > 255 || result < 0) {
            throw new IllegalArgumentException("Invalid byte numeric " +  result);
        }
        dest[destOffset] = (byte)result;
        return 1;
    }

    @Override
    public int partition(View value, int numPartitions) {
        return value.array[value.offset] % numPartitions;
    }

    @Override
    public String asString(byte[] src, int offset) {
        return String.valueOf(src[offset]);
    }

    @Override
    public int sizeOf(View value) {
        return 1;
    }

}

