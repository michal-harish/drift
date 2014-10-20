package net.imagini.aim.segment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public interface AimSegment {

    void appendRecord(ByteBuffer record) throws IOException;
    void appendRecord(byte[][] record) throws IOException;
    void appendRecord(String... values) throws IOException;

    void close() throws IllegalAccessException, IOException;

    long getCount();

    long getCompressedSize();

    long getOriginalSize();

    Long count(AimFilter filter)  throws IOException;

    InputStream select(AimFilter filter, String[] columnNames) throws IOException;

}
