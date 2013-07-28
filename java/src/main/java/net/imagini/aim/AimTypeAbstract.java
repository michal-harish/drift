package net.imagini.aim;


abstract public class AimTypeAbstract implements AimType {

    public interface AimDataType extends AimType {
        int getSize();
    }

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
