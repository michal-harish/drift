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

    @Override
    public int convert(String value,  byte[] dest, int destOffset) {
        try {
            UUID uuid = UUID.fromString(value);
            ByteUtils.putLongValue(uuid.getMostSignificantBits(), dest, destOffset);
            ByteUtils.putLongValue(uuid.getLeastSignificantBits(), dest, destOffset + 8);
            return 16;
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }

    }

    @Override public byte[] convert(String value) {
        try {
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
            byte[] b = new byte[16];
            ByteUtils.putLongValue(mostSigBits, b, 0);
            ByteUtils.putLongValue(leastSigBits, b, 8);
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
