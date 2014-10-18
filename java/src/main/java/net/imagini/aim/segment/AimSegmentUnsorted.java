package net.imagini.aim.segment;

import java.io.IOException;
import java.nio.ByteBuffer;

import net.imagini.aim.types.AimSchema;
import net.imagini.aim.utils.BlockStorage;

final public class AimSegmentUnsorted extends AimSegmentAbstract {


    public AimSegmentUnsorted(AimSchema schema, Class<? extends BlockStorage> storageType) throws InstantiationException, IllegalAccessException {
        super(schema, storageType);
    }

    @Override public void append(ByteBuffer record) throws IOException {
        appendRecord(record);
    }

}
