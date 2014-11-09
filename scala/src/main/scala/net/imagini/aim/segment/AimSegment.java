package net.imagini.aim.segment;

import java.io.IOException;
import java.nio.ByteBuffer;

import net.imagini.aim.types.AimSchema;
import net.imagini.aim.utils.BlockStorage;
import net.imagini.aim.utils.View;

public interface AimSegment {

    BlockStorage getBlockStorage(int column);

    AimSegment appendRecord(View[] record) throws IOException;
    AimSegment appendRecord(String... values) throws IOException; 
    AimSegment appendRecord(ByteBuffer record) throws IOException;

    AimSegment close() throws IllegalAccessException, IOException;

    long count();

    long getCompressedSize();

    long getOriginalSize();

    AimSchema getSchema();

}
