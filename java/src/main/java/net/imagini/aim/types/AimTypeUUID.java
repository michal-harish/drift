package net.imagini.aim.types;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import net.imagini.aim.utils.AimUtils;

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
        return new UUID(AimUtils.getLongValue(value,0) , AimUtils.getLongValue(value,8)).toString();
    }

}
