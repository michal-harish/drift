package net.imagini.aim.types;

import java.nio.ByteBuffer;
import java.util.Arrays;

import net.imagini.aim.utils.ByteUtils;

public class AimTypeBYTEARRAY extends AimTypeAbstract implements AimDataType {
    final public int size;
    public AimTypeBYTEARRAY(int size) {  this.size = size; }
    @Override public int getLen() { return size; }
    @Override public int sizeOf(ByteBuffer value) { return size; }
    @Override public String toString() { return "BYTEARRAY["+size+"]"; }
    @Override public String escape(String value) { return "'" + value +"'"; }

    @Override public boolean equals(Object object) {
        return object instanceof AimTypeBYTEARRAY && this.size == ((AimTypeBYTEARRAY)object).getLen();
    }

    @Override public String asString(ByteBuffer value) {
        return new String(value.array(), value.position(),size);
    }

    @Override public int partition(ByteBuffer value, int numPartitions) {
        return ByteUtils.sum(value.array(), value.position(), size)  % numPartitions;
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
        return new String(value,0,size);
    }

    @Override final public AimDataType getDataType() {
        return this;
    }

}