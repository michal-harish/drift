package net.imagini.aim;

import java.io.IOException;
import java.io.InputStream;

public interface AimSegment {

    long getSize();

    long getOriginalSize();

    void close() throws IllegalAccessException, IOException;

    void write(int col, AimDataType type, byte[] value) throws IOException;

    InputStream open(int column) throws IOException;

}
