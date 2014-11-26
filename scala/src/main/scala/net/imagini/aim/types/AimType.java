package net.imagini.aim.types;

import net.imagini.aim.utils.View;


abstract public class AimType {

    abstract public int getLen();

    abstract public int sizeOf(View value);

    abstract public int partition(View value, int numPartitions);

    abstract public String asString(View value);

//    abstract public byte[] parse(View value, byte[] dest, int destOffset);

    abstract public int convert(String value, byte[] dest, int destOffset); 

    abstract public String convert(byte[] value);

    abstract public byte[] convert(String value);

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
}
