package net.imagini.aim.types;

import java.util.Arrays;

import net.imagini.aim.utils.View;


abstract public class AimType {

    abstract public int getLen();

    abstract public int sizeOf(View value);

    abstract public int partition(View value, int numPartitions);

    abstract public String asString(byte[] src, int offset);

    abstract public int parse(View value, byte[] dest, int destOffset);

    private final String id;

    public AimType() {
        this.id = this.getClass().getName().substring(AimType.class.getName().length());
    }

    @Override
    public String toString() {
        return id;
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof AimType && this.equals(((AimType)object));
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    public String escape(String value) {
        return value;
    }

    final public String asString(byte[] src) {
        if (src == null) {
            return Aim.EMPTY;
        } else {
            return asString(src, 0);
        }
    }
    final public String asString(View value) {
        if (value == null) { //TODO remove this check and make sure that View can never be null
            return Aim.EMPTY;
        } else {
            return asString(value.array, value.offset);
        }
    }

    final public byte[] convert(String value) {
        int len = getLen();
        if (len == -1) len = value.length() + 4;
        byte[] result = new byte[len];
        convert(value, result, 0);
        return result;
    }

    final public int convert(String value, byte[] dest, int destOffset) {
        int len = getLen();
        if (value == null || value.isEmpty()) {
            if (len == -1 ) len = 4;
            Arrays.fill(dest, destOffset, destOffset + len, (byte)0);
            return len;
        } else {
            return parse(new View(value.getBytes()), dest, destOffset);
        }
    }
}
