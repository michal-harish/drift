package net.imagini.drift.types;

import net.imagini.drift.utils.View;

public class AimTypeBOOL extends AimType {

    @Override
    public int getLen() {
        return 1;
    }

    @Override
    public int parse(View value, byte[] dest, int destOffset) {
        int len = value.limit - value.offset + 1;
        // assume '0' or '1' or 'true' or 'false'
        boolean yes = len == 4 ? true
                : (len == 1 ? value.array[value.offset] != '0' : false);
        dest[destOffset] = (byte) (yes ? 1 : 0);
        return 1;
    }

    @Override
    public int partition(View value, int numPartitions) {
        return value.array[value.offset] % numPartitions;
    }

    @Override
    public String asString(byte[] src, int offset) {
        return String.valueOf(src[offset] > 0);
    }

    @Override
    public int sizeOf(View value) {
        return 1;
    }

}
