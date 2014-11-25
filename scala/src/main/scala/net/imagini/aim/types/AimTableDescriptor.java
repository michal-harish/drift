package net.imagini.aim.types;

import net.imagini.aim.segment.AimSegment;
import net.imagini.aim.segment.AimSegmentQuickSort;
import net.imagini.aim.utils.BlockStorage;

public class AimTableDescriptor {
    public final AimSchema schema;
    public final AimType keyType;
    public final int segmentSize;
    public final Class<? extends BlockStorage> storageType;
    public final Class<? extends AimSegment> sortType;

    public AimTableDescriptor(String descriptor) throws ClassNotFoundException {
        this(
                AimSchema.fromString(descriptor.split("\n")[0]), 
                java.lang.Integer.valueOf(descriptor.split("\n")[1]), 
                Class.forName(descriptor.split("\n")[2]).asSubclass(BlockStorage.class),
                AimSegmentQuickSort.class);
    }

    public AimTableDescriptor(AimSchema schema, int segmentSize,
            Class<? extends BlockStorage> storageType,
            Class<? extends AimSegment> sortType) {
        this.schema = schema;
        this.keyType = schema.get(0);
        this.segmentSize = segmentSize;
        this.storageType = storageType;
        this.sortType = sortType;

    }

    @Override
    public String toString() {
        return schema.toString() + "\n" + String.valueOf(segmentSize) + "\n"
                + storageType.getName();
    }
}
