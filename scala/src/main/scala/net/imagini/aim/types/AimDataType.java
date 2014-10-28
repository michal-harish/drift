package net.imagini.aim.types;

import java.nio.ByteBuffer;

public interface AimDataType extends AimType {
    int getSize();
    int sizeOf(ByteBuffer value);
}
