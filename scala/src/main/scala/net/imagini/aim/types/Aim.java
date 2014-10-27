package net.imagini.aim.types;

import java.nio.ByteBuffer;

import net.imagini.aim.utils.ByteUtils;

public enum Aim implements AimDataType {

    EMPTY(0),
    BOOL(1),
    BYTE(1),
    INT(4),
    LONG(8),
    STRING(4);

    public static AimDataType BYTEARRAY(int size) {  return new AimTypeBYTEARRAY(size); }
    public static AimType IPV4(AimDataType dataType) { return new AimTypeIPv4(dataType); }
    public static AimType UUID(AimDataType dataType) { return new AimTypeUUID(dataType); }
    public static AimType TIME(AimDataType dataType) { return new AimTypeTIME(dataType); }
    //TODO AimType UTF8(4) extends AimTypeAbstract implements AimDataType

    private Aim(int size) {
        this.size = size;
    }

    final public int size;

    @Override public int getSize() {
        return size;
    }

    @Override final public AimDataType getDataType() {
        return this;
    }

    @Override public String escape(String value) {
        return this.equals(STRING) ? "'"+value+"'" : value;
    }

    @Override
    public byte[] convert(String value) {
        ByteBuffer bb;
        if (this.equals(Aim.EMPTY)) {
            return null;
        } else if (this.equals(Aim.BOOL)) {
            bb = ByteBuffer.allocate(1);
            bb.put((byte) (Boolean.valueOf(value) ? 1 : 0));
        } else if (this.equals(Aim.BYTE)) {
            bb = ByteBuffer.allocate(1);
            bb.put(Byte.valueOf(value));
        } else if (this.equals(Aim.INT)) {
            bb = ByteUtils.createIntBuffer(Integer.valueOf(value));
        } else if (this.equals(Aim.LONG)) {
            bb = ByteUtils.createLongBuffer(Long.valueOf(value));
        } else if (this.equals(Aim.STRING)) {
            bb = ByteUtils.createStringBuffer(value);
        } else {
            throw new IllegalArgumentException("Unknown data type " + this.getClass().getSimpleName());
        }
        return bb.array();
    }

    @Override
    public String convert(byte[] value) {
        if (this.equals(Aim.EMPTY)) {
            return "-";
        } else if (this.equals(Aim.BOOL)) {
            return String.valueOf(value[0]>0);
        } else if (this.equals(Aim.BYTE)) {
            return String.valueOf(value[0]);
        } else if (this.equals(Aim.INT)) {
            return String.valueOf(ByteUtils.getIntValue(value));
        } else if (this.equals(Aim.LONG)) {
            return String.valueOf(ByteUtils.getLongValue(value,0));
        } else if (this.equals(Aim.STRING)) {
            int size = ByteUtils.getIntValue(value);
            return new String(value, 4, size);
        } else {
            throw new IllegalArgumentException("Unknown type " + this);
        }
    }

}
