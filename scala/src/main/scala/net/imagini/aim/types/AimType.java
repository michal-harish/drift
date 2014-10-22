package net.imagini.aim.types;


/**
 * AimType is any primitive or complex type while AimDataType is how it is represented
 * on a storage level. Therefore data types are fixed primitves whil types can be
 * constructed out of them and other types.
 * 
 * @author mharis
 *
 */
public interface AimType {

    public AimDataType getDataType();

    public byte[] convert(String value);

    public String convert(byte[] value);

    public String wrap(String value);

}
