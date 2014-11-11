package net.imagini.aim.segment;

import java.io.IOException;

import net.imagini.aim.types.AimSchema;
import net.imagini.aim.utils.BlockStorage;

final public class AimSegmentUnsorted extends AimSegmentAbstract {

    public AimSegmentUnsorted(AimSchema schema,
            Class<? extends BlockStorage> storageType)
            throws InstantiationException, IllegalAccessException {
        super(schema, storageType);
    }

    @Override
    public AimSegment appendRecord(byte[][] record) throws IOException {
        commitRecord(record);
        return this;
    }
}
