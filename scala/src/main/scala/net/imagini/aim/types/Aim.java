package net.imagini.aim.types;

import net.imagini.aim.utils.ByteUtils;
import net.imagini.aim.utils.View;

public enum Aim implements AimDataType {

    BOOL(1), BYTE(1), INT(4), LONG(8), STRING(-1); //TODO DOUBLE(8), 

    final public static String EMPTY = String.valueOf((char) 0); //

    public static AimDataType BYTEARRAY(int size) {
        return new AimTypeBYTEARRAY(size);
    }

    public static AimType IPV4(AimDataType dataType) {
        return new AimTypeIPv4(dataType);
    }

    public static AimType UUID(AimDataType dataType) {
        return new AimTypeUUID(dataType);
    }

    public static AimType TIME(AimDataType dataType) {
        return new AimTypeTIME(dataType);
    }

    // TODO AimType UTF8(4) extends AimTypeAbstract implements AimDataType

    private Aim(int size) {
        this.size = size;
    }

    final private int size;

    @Override
    public int getLen() {
        return size;
    }

    @Override
    final public AimDataType getDataType() {
        return this;
    }

    @Override
    public String escape(String value) {
        return this.equals(STRING) ? "'" + value + "'" : value;
    }

    @Override
    public byte[] convert(String value) {
        if (value == null || value.equals(EMPTY)) {
            return null;
        }
        byte[] b;
        if (this.equals(Aim.BOOL)) {
            b = new byte[1];
            b[0] = (byte)(Boolean.valueOf(value) ? 1 : 0);
        } else if (this.equals(Aim.BYTE)) {
            b = new byte[1];
            b[0] = Byte.valueOf(value);
        } else if (this.equals(Aim.INT)) {
            b = new byte[4];
            ByteUtils.putIntValue(Integer.valueOf(value), b, 0);
        } else if (this.equals(Aim.LONG)) {
            b = new byte[8];
            ByteUtils.putLongValue(Long.valueOf(value), b, 0);
        } else if (this.equals(Aim.STRING)) {
            b = new byte[value.length() + 4];
            ByteUtils.putIntValue(value.length(), b, 0);
            value.getBytes(0, value.length(), b, 4);
            //TODO this deprecated method is a reminder of UTF-8 and other chars larger than byte
        } else {
            throw new IllegalArgumentException("Unknown data type "
                    + this.getClass().getSimpleName());
        }
        return b;
    }

    @Override
    public int sizeOf(View value) {
        if (this.size == -1) {
            return ByteUtils.asIntValue(value.array, value.offset) + 4;
        } else {
            return size;
        }
    }

    @Override
    public int partition(View value, int numPartitions) {
        switch (this) {
            case BOOL: 
            case BYTE: return value.array[value.offset] % numPartitions;
            case INT: return ByteUtils.asIntValue(value.array, value.offset) % numPartitions;
            case LONG: return (int)ByteUtils.asLongValue(value.array, value.offset) % numPartitions;
            case STRING: return Math.abs(ByteUtils.crc32(value.array, value.offset + 4, ByteUtils.asIntValue(value.array, value.offset))) % numPartitions; 
            default: throw new IllegalArgumentException();
        }
    }

    @Override
    public String convert(byte[] value) {
        if (value == null) {
            return EMPTY;
        }
        switch (this) {
        case BOOL:
            return String.valueOf(value[0] > 0);
        case BYTE:
            return String.valueOf(value[0]);
        case INT:
            return String.valueOf(ByteUtils.asIntValue(value));
        case LONG:
            return String.valueOf(ByteUtils.asLongValue(value, 0));
        case STRING:
            return new String(value, 4, ByteUtils.asIntValue(value));
        default:
            throw new IllegalArgumentException("Unknown type " + this);
        }
    }

    @Override
    public String asString(View view) {
        if (view == null) {
            return EMPTY;
        } else
            switch (this) {
            case BOOL:
                return String.valueOf(view.array[view.offset] > 0);
            case BYTE:
                return String.valueOf(view.offset);
            case INT:
                return String.valueOf(ByteUtils.asIntValue(view.array, view.offset));
            case LONG:
                return String.valueOf(ByteUtils.asLongValue(view.array, view.offset));
            case STRING:
                return new String(view.array, view.offset + 4, ByteUtils.asIntValue(view.array, view.offset));
            default:
                return "";
            }
    }

}
