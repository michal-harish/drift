package net.imagini.drift.types;

import net.imagini.drift.utils.ByteUtils;
import net.imagini.drift.utils.View;

public class DriftTypeUUID extends DriftTypeBYTEARRAY {

    public DriftTypeUUID() {
        super(16);
    }

    @Override
    public int parse(View value, byte[] dest, int destOffset) {
        ByteUtils.parseUUID(value.array, value.offset, dest, destOffset);
        /*
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
        */
        return 16;
    }

    @Override public String toString() { 
        return "UUID"; 
    }

    @Override public String asString(byte[] src, int offset) {
        return UUIDToString(ByteUtils.asLongValue(src, offset) , ByteUtils.asLongValue(src, offset+8));
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
