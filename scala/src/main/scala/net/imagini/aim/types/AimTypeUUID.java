package net.imagini.aim.types;

import java.util.Arrays;
import java.util.UUID;

import net.imagini.aim.utils.ByteUtils;
import net.imagini.aim.utils.View;

public class AimTypeUUID extends AimTypeAbstract {

    private AimDataType dataType;
    public AimTypeUUID(AimDataType dataType) {
        if (!dataType.equals(Aim.BYTEARRAY(16))) {
            throw new IllegalArgumentException("Unsupported data type `"+dataType+"` for type AimTypeUUID");
        }
        this.dataType = dataType;
    }
    @Override public String toString() { return "UUID:"+dataType.toString(); }
    @Override public AimDataType getDataType() {
        return dataType;
    }
    @Override public byte[] convert(String value) {
        try {
            UUID uuid = UUID.fromString(value);
            byte[] b = new byte[16];
            ByteUtils.putLongValue(uuid.getMostSignificantBits(), b, 0);
            ByteUtils.putLongValue(uuid.getLeastSignificantBits(), b, 8);
            return b;
        } catch (Exception e) {
            byte[] result = new byte[16];
            Arrays.fill(result, (byte)0);
            return result;
        }

    }

    @Override public String convert(byte[] value) {
        return new UUID(ByteUtils.asLongValue(value,0) , ByteUtils.asLongValue(value,8)).toString();
    }

    @Override public String asString(View view) {
        return new UUID(ByteUtils.asLongValue(view.array, view.offset) , ByteUtils.asLongValue(view.array, view.offset+8)).toString();
    }
    @Override public String escape(String value) {
        return "'"+value+"'";
    }
    @Override public int partition(View view, int numPartitions) {
        long hilo = ByteUtils.asLongValue(view.array, view.offset) ^ ByteUtils.asLongValue(view.array, view.offset+8);
        int hash = ((int)(hilo >> 32)) ^ (int) hilo;
        return (hash == Integer.MIN_VALUE ? Integer.MAX_VALUE : Math.abs(hash)) % numPartitions;
    }
}
