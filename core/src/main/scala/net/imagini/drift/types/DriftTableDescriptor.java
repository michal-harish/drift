package net.imagini.drift.types;

import net.imagini.drift.utils.BlockStorage;

public class DriftTableDescriptor {

    public final DriftSchema schema;
    public final DriftType keyType;
    public final int segmentSize;
    public final Class<? extends BlockStorage> storageType;
    public final SortType sortType;

    public DriftTableDescriptor(String descriptor) throws ClassNotFoundException {
        this(
                DriftSchema.fromString(descriptor.split("\n")[0]), 
                java.lang.Integer.valueOf(descriptor.split("\n")[1]), 
                Class.forName(descriptor.split("\n")[2]).asSubclass(BlockStorage.class),
                SortType.QUICK_SORT);
    }

    public DriftTableDescriptor(DriftSchema schema, int segmentSize,
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
