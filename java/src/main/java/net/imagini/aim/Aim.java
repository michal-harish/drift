package net.imagini.aim;


public enum Aim implements AimDataType {
    BOOL(1),
    BYTE(1),
    INT(4),
    LONG(8),
    STRING(4);
    public static AimDataType BYTEARRAY(int size) {
        return new BYTEARRAY(size);
    }
    public static class BYTEARRAY implements AimDataType {
        final public int size;
        private BYTEARRAY(int size) {  this.size = size; }
        @Override public int getSize() { return size; }
        @Override public String toString() { return "BYTEARRAY("+size+")"; }
    }
    final public int size;
    private Aim(int size) {
        this.size = size;
    }
    @Override public int getSize() {
        return size;
    }
}
