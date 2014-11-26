package net.imagini.aim.types;


public class Aim {

    public static final String EMPTY = String.valueOf((char) 0);

    public static final AimType BOOL = new AimTypeBOOL();
    public static final AimType BYTE = new AimTypeBYTE();
    public static final AimType INT = new AimTypeINT();
    public static final AimType LONG = new AimTypeLONG();
    public static final AimType STRING = new AimTypeSTRING();
    public static final AimType IPV4 = new AimTypeIPV4();
    public static final AimType UUID = new AimTypeUUID();
    public static final AimType TIME = new AimTypeTIME();
    //TODO public static final AimType DOUBLE = new AimTypeDOUBLE();

    public static AimType BYTEARRAY(int size) { return new AimTypeBYTEARRAY(size); }


}
