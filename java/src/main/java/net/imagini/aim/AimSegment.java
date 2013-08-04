package net.imagini.aim;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public interface AimSegment {

    void append(ByteBuffer records) throws IOException;

    void close() throws IllegalAccessException, IOException;

    long getCount();

    long getSize();

    long getOriginalSize();

    Long count(AimFilter filter)  throws IOException;

    InputStream open(AimFilter filter, String[] columnNames) throws IOException;

}
