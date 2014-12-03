package net.imagini.drift.types;

public class Drift {

    public static final String EMPTY = String.valueOf((char) 0);

    public static final DriftTypeBOOL BOOL = new DriftTypeBOOL();
    public static final DriftTypeBYTE BYTE = new DriftTypeBYTE();
    public static final DriftTypeINT INT = new DriftTypeINT();
    public static final DriftTypeLONG LONG = new DriftTypeLONG();
    public static final DriftTypeSTRING STRING = new DriftTypeSTRING();
    public static final DriftTypeIPV4 IPV4 = new DriftTypeIPV4();
    public static final DriftTypeUUID UUID = new DriftTypeUUID();
    public static final DriftTypeTIME TIME = new DriftTypeTIME();
    //TODO public static final DriftType DOUBLE = new DriftTypeDOUBLE();

    public static DriftTypeBYTEARRAY BYTEARRAY(int size) { return new DriftTypeBYTEARRAY(size); }

}
