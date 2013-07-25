package net.imagini.aim;

import net.imagini.aim.AimTypeAbstract.AimDataType;

public interface AimType {

    public AimDataType getDataType();

    public byte[] convert(String value);

    public String convert(byte[] value);

}
