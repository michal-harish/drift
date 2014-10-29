package net.imagini.aim.segment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import net.imagini.aim.tools.RowFilter;
import net.imagini.aim.tools.ColumnScanner;
import net.imagini.aim.types.AimSchema;

public interface AimSegment {

    AimSegment appendRecord(ByteBuffer record) throws IOException;
    AimSegment appendRecord(byte[][] record) throws IOException;
    AimSegment appendRecord(String... values) throws IOException;

    AimSegment close() throws IllegalAccessException, IOException;

    long count();

    long getCompressedSize();

    long getOriginalSize();

    long count(RowFilter filter)  throws IOException;

    InputStream select(RowFilter filter, String[] columnNames) throws IOException;

    ColumnScanner[] wrapScanners(AimSchema subSchema);

}
