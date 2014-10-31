package net.imagini.aim.types;

import java.nio.ByteBuffer;

//TODO AimDataType[T] public T view(ByteBuffer value)
public interface AimDataType extends AimType {
    int getSize();
    int sizeOf(ByteBuffer value);
    
}
