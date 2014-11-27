package net.imagini.aim.types;

import java.util.Arrays;

import net.imagini.aim.utils.ByteUtils;
import net.imagini.aim.utils.View;

public class AimTypeBYTEARRAY extends AimType {
    final public int size;
    public AimTypeBYTEARRAY(int size) {  this.size = size; }
    @Override public int getLen() { 
        return size; 
    }
    @Override
    public int parse(View value, byte[] dest, int destOffset) {
        ByteUtils.copy(value.array, value.offset, dest, destOffset, size);
        return size;
    }
    @Override public int sizeOf(View view) { 
        return size; 
    }
    @Override public String toString() { 
        return "BYTEARRAY["+size+"]"; 
    }
    @Override public String escape(String value) { 
        return "'" + value +"'"; 
    }

    @Override public boolean equals(Object object) {
        return object instanceof AimTypeBYTEARRAY && this.size == ((AimTypeBYTEARRAY)object).getLen();
    }

    @Override public String asString(View view) {
        return new String(view.array, view.offset, size);
    }

    @Override public int partition(View view, int numPartitions) {
        return ByteUtils.sum(view.array, view.offset, size)  % numPartitions;
    }

    @Override
    public int convert(String value,  byte[] dest, int destOffset) {
        return ByteUtils.copy(value.getBytes(), 0, dest, destOffset, size);
    }

    @Override public byte[] convert(String value) {
        byte[] bytes = new byte[size];
        Arrays.fill(bytes, (byte)0);
        byte[] val = value.getBytes();
        Arrays.copyOfRange(val, 0, Math.min(size, val.length));
        return bytes;
    }

    @Override public String convert(byte[] value) {
        return new String(value,0,size);
    }

}