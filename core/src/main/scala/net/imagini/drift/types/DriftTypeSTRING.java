package net.imagini.drift.types;

import net.imagini.drift.utils.ByteUtils;
import net.imagini.drift.utils.View;

public class DriftTypeSTRING extends DriftType {

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
    public String asString(byte[] src, int offset) {
        if (src == null) {
            return Drift.EMPTY;
        } else {
            return new String(src, offset + 4, ByteUtils.asIntValue(src, offset));
        }
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
