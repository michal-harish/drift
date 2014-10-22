package net.imagini.aim.types;


public class AimTypeTIME extends AimTypeAbstract {

    private AimDataType dataType;
    public AimTypeTIME(AimDataType dataType) {
        if (!dataType.equals(Aim.LONG)) {
            throw new IllegalArgumentException("Unsupported data type `"+dataType+"` for type AimTypeTime");
        }
        this.dataType = dataType;
    }
    @Override
    public AimDataType getDataType() {
        return dataType;
    }

    @Override public String toString() { return "TIME:"+dataType.toString(); }

    @Override
    public byte[] convert(String value) {
        //TODO parse timestamp and return long bytes
        return null;
    }

    @Override
    public String convert(byte[] value) {
        //formatter.format()
        return null;
    }
    
}