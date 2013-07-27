package net.imagini.aim;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

import net.imagini.aim.AimTypeAbstract.AimDataType;
import net.imagini.aim.pipes.Pipe;
import scala.actors.threadpool.Arrays;


public enum Aim implements AimDataType {
    BOOL(1),
    BYTE(1),
    INT(4),
    LONG(8),
    STRING(4);
    public static AimDataType STRING(int size) {  return new STRING(size); }
    public static AimType IPV4(AimDataType dataType) { return new AimTypeIPv4(dataType); }
    public static AimType UUID(AimDataType dataType) { return new AimTypeUUID(dataType); }
    //TODO AimType UTF8(4) extends AimTypeAbstract implements AimDataType

    public static enum SortOrder {
        ASC(-1),
        DESC(1);
        private int cmp;
        private SortOrder(int cmp) { this.cmp = cmp; }
        public int getComparator() { return cmp; }
    }

    /**
     * We need to use BIG_ENDIAN because the pro(s) are favourable 
     * to our usecase, e.g. lot of streaming filtering.
     */
    final public static ByteOrder endian = ByteOrder.BIG_ENDIAN;

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
            bb = ByteBuffer.allocate(val.length);
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
            return String.valueOf(Pipe.readInteger(value));
        } else if (this.equals(Aim.LONG)) {
            return String.valueOf(Pipe.readLong(value,0));
        } else if (this.equals(Aim.STRING)) {
            return new String(value);
        } else {
            throw new IllegalArgumentException("Unknown type " + this);
        }

    }

    public static class STRING extends AimTypeAbstract implements AimDataType {
        final public int size;
        private STRING(int size) {  this.size = size; }
        @Override public int getSize() { return size; }
        @Override public String toString() { return "STRING["+size+"]"; }
        @Override public boolean equals(Object object) {
            return object instanceof STRING && this.size == ((STRING)object).getSize();
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
            return new String(value); // TODO remove trailing zeros;
        }

        @Override final public AimDataType getDataType() {
            return this;
        }
    }

    public static class AimTypeUUID extends AimTypeAbstract {

        private AimDataType dataType;
        public AimTypeUUID(AimDataType dataType) {
            if (!dataType.equals(Aim.STRING(16))) {
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
            return new UUID(Pipe.readLong(value,0) , Pipe.readLong(value,8)).toString();
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
