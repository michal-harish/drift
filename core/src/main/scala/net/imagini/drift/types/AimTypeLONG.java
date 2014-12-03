package net.imagini.drift.types;

import net.imagini.drift.utils.ByteUtils;
import net.imagini.drift.utils.View;

public class AimTypeLONG extends AimType {

    @Override
    public int getLen() {
        return 8;
    }

    @Override
    public int parse(View value, byte[] dest, int destOffset) {
        long result = ByteUtils.parseLongRadix10(value.array, value.offset, value.limit);
        ByteUtils.putLongValue(result, dest, destOffset);
        return 8;
    }

    @Override
    public int partition(View value, int numPartitions) {
        return (int)ByteUtils.asLongValue(value.array, value.offset) % numPartitions;
    }

    @Override
    public String asString(byte[] src, int offset) {
        return String.valueOf(ByteUtils.asLongValue(src, offset));
    }

    @Override
    public int sizeOf(View value) {
        return 8;
    }

}