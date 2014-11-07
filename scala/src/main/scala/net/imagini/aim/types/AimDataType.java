package net.imagini.aim.types;

import net.imagini.aim.utils.View;

public interface AimDataType extends AimType {
    int getLen();
    int sizeOf(View value);
}
