package net.imagini.aim.types;

import java.nio.ByteBuffer;



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

    //TODO void parse(String value, ByteBuffer into);
    public byte[] convert(String value);

    public String asString(ByteBuffer value);

    public String convert(byte[] value);

    public String escape(String value);

}
