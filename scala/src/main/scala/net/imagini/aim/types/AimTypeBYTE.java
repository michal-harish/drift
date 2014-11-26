package net.imagini.aim.types;

import net.imagini.aim.utils.View;

public class AimTypeBYTE extends AimType {

    @Override
    public int getLen() {
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
    public int convert(String value, byte[] dest, int destOffset) {
        dest[destOffset] = Byte.valueOf(value);
        return 1; 
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
    public byte[] convert(String value) {
        byte[] b = new byte[1];
        b[0] = Byte.valueOf(value);
        return b;
    }

    @Override
    public int sizeOf(View value) {
        return 1;
    }

}

