package net.imagini.aim.types;

import java.nio.ByteBuffer;


abstract public class AimTypeAbstract implements AimType {

    @Override
    public boolean equals(Object object) {
        return object instanceof AimType && this.getDataType().equals(((AimType)object).getDataType());
    }
    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override public String escape(String value) {
        return value;
    }
    @Override public int partition(ByteBuffer value, int numPartitions) {
        return getDataType().partition(value, numPartitions);
    }
}
