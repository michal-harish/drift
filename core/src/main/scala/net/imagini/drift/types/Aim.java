package net.imagini.drift.types;

public class Aim {

    public static final String EMPTY = String.valueOf((char) 0);

    public static final AimTypeBOOL BOOL = new AimTypeBOOL();
    public static final AimTypeBYTE BYTE = new AimTypeBYTE();
    public static final AimTypeINT INT = new AimTypeINT();
    public static final AimTypeLONG LONG = new AimTypeLONG();
    public static final AimTypeSTRING STRING = new AimTypeSTRING();
    public static final AimTypeIPV4 IPV4 = new AimTypeIPV4();
    public static final AimTypeUUID UUID = new AimTypeUUID();
    public static final AimTypeTIME TIME = new AimTypeTIME();
    //TODO public static final AimType DOUBLE = new AimTypeDOUBLE();

    public static AimType BYTEARRAY(int size) { return new AimTypeBYTEARRAY(size); }

}
