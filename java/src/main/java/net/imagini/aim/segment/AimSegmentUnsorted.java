package net.imagini.aim.segment;

import net.imagini.aim.types.AimSchema;
import net.imagini.aim.utils.BlockStorage;

final public class AimSegmentUnsorted extends AimSegmentAbstract {


    public AimSegmentUnsorted(AimSchema schema, Class<? extends BlockStorage> storageType) 
    throws InstantiationException, IllegalAccessException {
        super(schema, null, storageType);
    }

}
