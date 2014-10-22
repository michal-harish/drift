package net.imagini.aim.types;


abstract public class AimTypeAbstract implements AimType {

    @Override
    public boolean equals(Object object) {
        return object instanceof AimType && this.getDataType().equals(((AimType)object).getDataType());
    }
    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override public String wrap(String value) {
        return value;
    }

}
