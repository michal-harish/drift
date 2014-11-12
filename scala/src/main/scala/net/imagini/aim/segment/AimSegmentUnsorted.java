package net.imagini.aim.segment;

import java.io.IOException;

import net.imagini.aim.types.AimSchema;

final public class AimSegmentUnsorted extends AimSegment {

    public AimSegmentUnsorted(AimSchema schema) {
        super(schema);
    }
    @Override
    public AimSegment appendRecord(byte[][] record) throws IOException {
        commitRecord(record);
        return this;
    }
}
