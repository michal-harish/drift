package net.imagini.aim.types;

import net.imagini.aim.utils.View;



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

    public String asString(View value);

    public int partition(View value, int numPartitions);

    public String convert(byte[] value);

    public String escape(String value);

    public byte[] convert(String value);

}
