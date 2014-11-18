package net.imagini.aim.segment;

import java.io.IOException;

import net.imagini.aim.types.AimSchema;

final public class AimSegmentUnsorted extends AimSegment {

    private int recordedSize = 0;
    public AimSegmentUnsorted(AimSchema schema) {
        super(schema);
    }
    @Override
    public AimSegment appendRecord(byte[][] record) throws IOException {
        commitRecord(record);
        for(byte[] r: record) {
            recordedSize += r.length;
        }
        return this;
    }

    @Override public int getRecordedSize() {
        return recordedSize;
    }
}
