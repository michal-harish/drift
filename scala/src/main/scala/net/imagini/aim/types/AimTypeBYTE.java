package net.imagini.aim.types;

import net.imagini.aim.utils.ByteUtils;
import net.imagini.aim.utils.View;

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
    public String asString(View value) {
        return String.valueOf(value.array[value.offset]);
    }

    @Override
    public String convert(byte[] value) {
        return String.valueOf(value[0]);
    }

    @Override
    public String escape(String value) {
        return value;
    }

    @Override
    public int sizeOf(View value) {
        return 1;
    }

}

