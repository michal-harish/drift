package net.imagini.drift.types;

import net.imagini.drift.utils.BlockStorage;

public class AimTableDescriptor {

    public final AimSchema schema;
    public final AimType keyType;
    public final int segmentSize;
    public final Class<? extends BlockStorage> storageType;
    public final SortType sortType;

    public AimTableDescriptor(String descriptor) throws ClassNotFoundException {
        this(
                AimSchema.fromString(descriptor.split("\n")[0]), 
                java.lang.Integer.valueOf(descriptor.split("\n")[1]), 
                Class.forName(descriptor.split("\n")[2]).asSubclass(BlockStorage.class),
                SortType.QUICK_SORT);
    }

    public AimTableDescriptor(AimSchema schema, int segmentSize,
            Class<? extends BlockStorage> storageType,
            SortType sortType) {
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
