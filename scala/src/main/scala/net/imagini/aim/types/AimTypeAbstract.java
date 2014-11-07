package net.imagini.aim.types;

import net.imagini.aim.utils.View;


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
    @Override public int partition(View view, int numPartitions) {
        return getDataType().partition(view, numPartitions);
    }
}
