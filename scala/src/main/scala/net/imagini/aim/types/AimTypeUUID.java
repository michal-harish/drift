package net.imagini.aim.types;

import net.imagini.aim.utils.ByteUtils;
import net.imagini.aim.utils.View;

public class AimTypeUUID extends AimTypeBYTEARRAY {

    public AimTypeUUID() {
        super(16);
    }

    @Override
    public int parse(View value, byte[] dest, int destOffset) {
        long mostSigBits = ByteUtils.parseLongRadix16(value.array, value.offset, value.offset + 7);
        mostSigBits <<= 16;
        mostSigBits |= ByteUtils.parseLongRadix16(value.array, value.offset+9, value.offset + 12);
        mostSigBits <<= 16;
        mostSigBits |= ByteUtils.parseLongRadix16(value.array, value.offset+14, value.offset + 17);
        long leastSigBits = ByteUtils.parseLongRadix16(value.array, value.offset+19, value.offset + 22);
        leastSigBits <<= 48;
        leastSigBits |= ByteUtils.parseLongRadix16(value.array, value.offset+24, value.offset + 35);
        ByteUtils.putLongValue(mostSigBits, dest, destOffset);
        ByteUtils.putLongValue(leastSigBits, dest, destOffset + 8);
        return 16;
    }

    @Override public String toString() { 
        return "UUID"; 
    }

    @Override
    public int convert(String value,  byte[] dest, int destOffset) {
        String[] components = value.split("-");
        if (components.length != 5)
            throw new IllegalArgumentException("Invalid UUID string: "+value);
        for (int i=0; i<5; i++)
            components[i] = "0x"+components[i];
        long mostSigBits = Long.decode(components[0]).longValue();
        mostSigBits <<= 16;
        mostSigBits |= Long.decode(components[1]).longValue();
        mostSigBits <<= 16;
        mostSigBits |= Long.decode(components[2]).longValue();
        long leastSigBits = Long.decode(components[3]).longValue();
        leastSigBits <<= 48;
        leastSigBits |= Long.decode(components[4]).longValue();
        ByteUtils.putLongValue(mostSigBits, dest, destOffset);
        ByteUtils.putLongValue(leastSigBits, dest, destOffset + 8);
        return 16;
    }

    @Override public byte[] convert(String value) {
        byte[] b = new byte[16];
        convert(value, b, 0);
        return b;
    }

    @Override public String convert(byte[] value) {
        long mostSigBits = ByteUtils.asLongValue(value,0);
        long leastSigBits = ByteUtils.asLongValue(value,8);
        return UUIDToString(mostSigBits, leastSigBits);
    }

    @Override public String asString(View view) {
        return UUIDToString(ByteUtils.asLongValue(view.array, view.offset) , ByteUtils.asLongValue(view.array, view.offset+8));
    }

    @Override public String escape(String value) {
        return "'"+value+"'";
    }

    @Override public int partition(View view, int numPartitions) {
        long hilo = ByteUtils.asLongValue(view.array, view.offset) ^ ByteUtils.asLongValue(view.array, view.offset+8);
        int hash = ((int)(hilo >> 32)) ^ (int) hilo;
        return (hash == Integer.MIN_VALUE ? Integer.MAX_VALUE : Math.abs(hash)) % numPartitions;
    }

    private static String UUIDToString(long mostSigBits, long leastSigBits) {
        return (digits(mostSigBits >> 32, 8) + "-" +
                digits(mostSigBits >> 16, 4) + "-" +
                digits(mostSigBits, 4) + "-" +
                digits(leastSigBits >> 48, 4) + "-" +
                digits(leastSigBits, 12));
    }
    private static String digits(long val, int digits) {
        long hi = 1L << (digits * 4);
        return Long.toHexString(hi | (val & (hi - 1))).substring(1);
    }

}
