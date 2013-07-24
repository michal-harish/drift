package net.imagini.aim;

import java.io.IOException;
import java.io.InputStream;
import java.util.BitSet;

public interface AimSegment {

    void write(int col, AimDataType type, byte[] value) throws IOException;

    void close() throws IllegalAccessException, IOException;

    long getSize();

    long getOriginalSize();

    InputStream open(int column) throws IOException;

    InputStream[] open(Integer... columnNames) throws IOException;

    Integer filter(AimFilter filter, BitSet segmentResult)  throws IOException; 

}
