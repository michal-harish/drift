package net.imagini.aim.types;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import net.imagini.aim.utils.ByteUtils;

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
            ByteBuffer bb = ByteBuffer.allocate(16); 
            bb.putLong(uuid.getMostSignificantBits());
            bb.putLong(uuid.getLeastSignificantBits());
            return bb.array();
        } catch (Exception e) {
            byte[] result = new byte[16];
            Arrays.fill(result, (byte)0);
            return result;
        }

    }

    @Override public String convert(byte[] value) {
        return new UUID(ByteUtils.asLongValue(value,0) , ByteUtils.asLongValue(value,8)).toString();
    }

    @Override public String asString(ByteBuffer value) {
        return new UUID(ByteUtils.asLongValue(value) , ByteUtils.asLongValue(value,8)).toString();
    }
    @Override public String escape(String value) {
        return "'"+value+"'";
    }
    @Override public int partition(ByteBuffer value, int numPartitions) {
        long hilo = ByteUtils.asLongValue(value) ^ ByteUtils.asLongValue(value,8);
        int hash = ((int)(hilo >> 32)) ^ (int) hilo;
        return (hash == Integer.MIN_VALUE ? Integer.MAX_VALUE : Math.abs(hash)) % numPartitions;
    }
}
