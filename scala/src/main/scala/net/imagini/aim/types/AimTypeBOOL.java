package net.imagini.aim.types;

import net.imagini.aim.utils.View;

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
    public String asString(View value) {
        return String.valueOf(value.array[value.offset] > 0);
    }


    @Override
    public String convert(byte[] value) {
        return String.valueOf(value[0] > 0);
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
