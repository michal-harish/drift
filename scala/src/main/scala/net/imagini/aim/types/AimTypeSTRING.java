package net.imagini.aim.types;

import net.imagini.aim.utils.ByteUtils;
import net.imagini.aim.utils.View;

public class AimTypeSTRING extends AimType {

    @Override
    public int getLen() {
        return -1;
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
    public int convert(String value, byte[] dest, int destOffset) {
        int len = value.length() + 4;
        ByteUtils.putIntValue(value.length(), dest, destOffset);
        value.getBytes(0, value.length(), dest, destOffset + 4);
        // TODO this deprecated method is a reminder of UTF-8 and other chars
        // larger than byte
        return len;
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
    public byte[] convert(String value) {
        byte[] b = new byte[value.length() + 4];
        ByteUtils.putIntValue(value.length(), b, 0);
        value.getBytes(0, value.length(), b, 4);
        // TODO this deprecated method is a reminder of UTF-8 and other chars
        // larger than byte
        return b;
    }

    @Override
    public int sizeOf(View value) {
        return ByteUtils.asIntValue(value.array, value.offset) + 4;
    }

}
