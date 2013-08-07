package net.imagini.aim;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.UUID;

import net.imagini.aim.AimTypeAbstract.AimDataType;


public enum Aim implements AimDataType {
    BOOL(1),
    BYTE(1),
    INT(4),
    LONG(8),
    STRING(4);
    public static AimDataType BYTEARRAY(int size) {  return new BYTEARRAY(size); }
    public static AimType IPV4(AimDataType dataType) { return new AimTypeIPv4(dataType); }
    public static AimType UUID(AimDataType dataType) { return new AimTypeUUID(dataType); }
    //TODO AimType UTF8(4) extends AimTypeAbstract implements AimDataType

    public static enum SortOrder {
        ASC(1),
        DESC(-1);
        private int cmp;
        private SortOrder(int cmp) { this.cmp = cmp; }
        public int getComparator() { return cmp; }
    }

    /**
     * We need to use BIG_ENDIAN because the pro(s) are favourable 
     * to our usecase, e.g. lot of streaming filtering.
     */
    final public static ByteOrder endian = ByteOrder.BIG_ENDIAN;

    /**
     * This is for ZeroCopy routines when they allocate mulit-dimensional buffers
     */
    final public static Integer COLUMN_BUFFER_SIZE = 2048; //FIXME 2048

    final public int size;

    private Aim(int size) {
        this.size = size;
    }

    @Override public int getSize() {
        return size;
    }

    @Override final public AimDataType getDataType() {
        return this;
    }

    @Override public String wrap(String value) {
        return this.equals(STRING) ? "'"+value+"'" : value;
    }

    @Override
    public byte[] convert(String value) {
        ByteBuffer bb;
        if (this.equals(Aim.BOOL)) {
            bb = ByteBuffer.allocate(1);
            bb.put((byte) (Boolean.valueOf(value) ? 1 : 0));
        } else if (this.equals(Aim.BYTE)) {
            bb = ByteBuffer.allocate(1);
            bb.put(Byte.valueOf(value));
        } else if (this.equals(Aim.INT)) {
            bb = ByteBuffer.allocate(4);
            bb.order(endian);
            bb.putInt(Integer.valueOf(value));
        } else if (this.equals(Aim.LONG)) {
            bb = ByteBuffer.allocate(8);
            bb.order(endian);
            bb.putLong(Long.valueOf(value));
        } else if (this.equals(Aim.STRING)) {
            byte[] val = value.getBytes();
            bb = ByteBuffer.allocate(val.length + 4);
            bb.order(endian);
            bb.putInt(value.length());
            bb.put(val);
        } else {
            throw new IllegalArgumentException("Unknown data type " + this.getClass().getSimpleName());
        }
        return bb.array();
    }

    @Override
    public String convert(byte[] value) {
        if (this.equals(Aim.BOOL)) {
            return String.valueOf(value[0]>0);
        } else if (this.equals(Aim.BYTE)) {
            return String.valueOf(value[0]);
        } else if (this.equals(Aim.INT)) {
            return String.valueOf(AimUtils.getIntegerValue(value));
        } else if (this.equals(Aim.LONG)) {
            return String.valueOf(AimUtils.getLongValue(value,0));
        } else if (this.equals(Aim.STRING)) {
            int size = AimUtils.getIntegerValue(value);
            return new String(value, 4, size);
        } else {
            throw new IllegalArgumentException("Unknown type " + this);
        }

    }

    public static class BYTEARRAY extends AimTypeAbstract implements AimDataType {
        final public int size;
        private BYTEARRAY(int size) {  this.size = size; }
        @Override public int getSize() { return size; }
        @Override public String toString() { return "BYTEARRAY["+size+"]"; }
        @Override public String wrap(String value) { return "'" + value +"'"; }

        @Override public boolean equals(Object object) {
            return object instanceof BYTEARRAY && this.size == ((BYTEARRAY)object).getSize();
        }
        @Override public byte[] convert(String value) {
            byte[] bytes = new byte[size];
            Arrays.fill(bytes, (byte)0);
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            byte[] val = value.getBytes();
            bb.put(val,0, Math.min(size,val.length));
            return bb.array();
        }

        @Override public String convert(byte[] value) {
            return new String(value,0,size);//FIXME clean 0 bytes
        }

        @Override final public AimDataType getDataType() {
            return this;
        }
    }

    public static class AimTypeUUID extends AimTypeAbstract {

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

    public static class AimTypeIPv4 extends AimTypeAbstract {

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

}
