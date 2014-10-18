package net.imagini.aim.segment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import net.imagini.aim.AimFilter;

public interface AimSegment {

    void append(ByteBuffer record) throws IOException;

    void close() throws IllegalAccessException, IOException;

    long getCount();

    long getCompressedSize();

    long getOriginalSize();

    Long count(AimFilter filter)  throws IOException;

    InputStream select(AimFilter filter, String keyField, String[] columnNames) throws IOException;

}
