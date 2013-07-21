package net.imagini.aim;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;


public enum Aim implements AimDataType {
    BOOL(1),
    BYTE(1),
    INT(4),
    LONG(8),
    STRING(4);
    public static AimDataType BYTEARRAY(int size) {
        return new BYTEARRAY(size);
    }
    public static class BYTEARRAY implements AimDataType {
        final public int size;
        private BYTEARRAY(int size) {  this.size = size; }
        @Override public int getSize() { return size; }
        @Override public String toString() { return "BYTEARRAY("+size+")"; }
    }
    final public int size;
    private Aim(int size) {
        this.size = size;
    }
    @Override public int getSize() {
        return size;
    }

    static public byte[] convert(AimDataType type, String value) {
        ByteBuffer bb;
        if (type.equals(BOOL)) {
            bb = ByteBuffer.allocate(1);
            bb.put((byte) (Boolean.valueOf(value) ? 1 : 0));
        } else if (type.equals(BYTE)) {
            bb = ByteBuffer.allocate(1);
            bb.put(Byte.valueOf(value));
        } else if (type.equals(INT)) {
            bb = ByteBuffer.allocate(4);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            bb.putInt(Integer.valueOf(value));
        } else if (type.equals(LONG)) {
            bb = ByteBuffer.allocate(8);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            bb.putLong(Long.valueOf(value));
        } else if (type.equals(STRING)) {
            bb = ByteBuffer.allocate(value.length());
            bb.put(value.getBytes());
        } else if (type instanceof BYTEARRAY) {
            int size = ((BYTEARRAY)type).size;
            if (value.length() != size) {
                throw new IllegalArgumentException("Invalid value for fixed-length byte array column; required size: " + size);
            }
            bb = ByteBuffer.allocate(size);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            bb.put(value.getBytes());
        } else {
            throw new IllegalArgumentException("Unknown data type " + type.getClass().getSimpleName());
        }
        return bb.array();
    }

}
