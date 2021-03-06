package net.imagini.drift.types;

import net.imagini.drift.utils.ByteUtils;
import net.imagini.drift.utils.View;

public class DriftTypeBYTEARRAY extends DriftType {
    final public int size;
    public DriftTypeBYTEARRAY(int size) {  this.size = size; }
    @Override public int getLen() { 
        return size; 
    }
    @Override
    public int parse(View value, byte[] dest, int destOffset) {
        ByteUtils.copy(value.array, value.offset, dest, destOffset, size); //TODO Math.min(size, val.length) Arrays.fill(bytes, (byte)0);
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
        return object instanceof DriftTypeBYTEARRAY && this.size == ((DriftTypeBYTEARRAY)object).getLen();
    }

    @Override public String asString(byte[] src, int offset) {
        return new String(src, offset, offset + size - 1);
    }

    @Override public int partition(View view, int numPartitions) {
        return ByteUtils.sum(view.array, view.offset, size)  % numPartitions;
    }

}