package net.imagini.aim.types;

import net.imagini.aim.utils.ByteUtils;
import net.imagini.aim.utils.View;

public class AimTypeSTRING extends AimType {

    @Override
    public int getLen() {
        return -1;
    }

    @Override
    public int parse(View value, byte[] dest, int destOffset) {
        int len = value.limit - value.offset + 1;
        ByteUtils.putIntValue(len, dest, destOffset);
        ByteUtils.copy(value.array, value.offset, dest, destOffset + 4, len);
        return len + 4;
    }

    @Override
    public int partition(View value, int numPartitions) {
        return Math.abs(ByteUtils.crc32(value.array, value.offset + 4,
                ByteUtils.asIntValue(value.array, value.offset)))
                % numPartitions;
    }

    @Override
    public String asString(View value) {
        if (value == null) {
            return Aim.EMPTY;
        } else {
            return new String(value.array, value.offset + 4, ByteUtils.asIntValue(value.array, value.offset));
        }
    }

    @Override
    public String convert(byte[] value) {
        return new String(value, 4, ByteUtils.asIntValue(value));
    }

    @Override
    public String escape(String value) {
        return "'" + value + "'";
    }

    @Override
    public int sizeOf(View value) {
        return ByteUtils.asIntValue(value.array, value.offset) + 4;
    }

}
