package net.imagini.aim.types;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class AimTypeIPv4 extends AimTypeAbstract {

    private AimDataType dataType;
    public AimTypeIPv4(AimDataType dataType) {
        if (!dataType.equals(Aim.INT)) {
            throw new IllegalArgumentException("Unsupported data type `"+dataType+"` for type AimTypeUUID");
        }
        this.dataType = dataType;
    }
    @Override public String toString() { return "IPV4:"+dataType.toString(); }
    @Override public AimDataType getDataType() {
        return dataType;
    }

    @Override public String asString(ByteBuffer value) {
        try {
            return InetAddress.getByAddress(Arrays.copyOfRange(value.array(), value.position(), 4)).toString();
        } catch (UnknownHostException e) {
            return null;
        }
    }

    @Override public byte[] convert(String value) {

        InetAddress clientIp;
        try {
            clientIp = InetAddress.getByName(value);
        } catch (UnknownHostException e) {
            byte[] result = new byte[4];
            Arrays.fill(result, (byte)0);
            return result;
        }
        return Arrays.copyOfRange(clientIp.getAddress(),0,4);

    }

    @Override public String convert(byte[] value) {
        try {
            return InetAddress.getByAddress(value).toString();
        } catch (UnknownHostException e) {
            return null;
        }
    }

}