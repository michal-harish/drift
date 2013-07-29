package net.imagini.aim;

import java.io.IOException;
import java.io.InputStream;
import java.util.BitSet;
import java.util.List;

import net.imagini.aim.pipes.Pipe;

public interface AimSegment {

    void append(Pipe pipe) throws IOException;

    void close() throws IllegalAccessException, IOException;

    long getCount();

    long getSize();

    long getOriginalSize();

    InputStream open(AimFilter filter, List<String> columnNames) throws IOException;

    Integer filter(AimFilter filter, BitSet segmentResult)  throws IOException; 

}
