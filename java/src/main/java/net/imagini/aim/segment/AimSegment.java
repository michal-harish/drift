package net.imagini.aim.segment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import net.imagini.aim.tools.AimFilter;

public interface AimSegment {

    void appendRecord(ByteBuffer record) throws IOException;
    void appendRecord(byte[][] record) throws IOException;
    void appendRecord(String... values) throws IOException;

    void close() throws IllegalAccessException, IOException;

    long count();

    long getCompressedSize();

    long getOriginalSize();

    long count(AimFilter filter)  throws IOException;

    InputStream select(AimFilter filter, String[] columnNames) throws IOException;

}
