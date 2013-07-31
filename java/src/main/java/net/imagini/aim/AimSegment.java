package net.imagini.aim;

import java.io.IOException;
import java.io.InputStream;

import net.imagini.aim.pipes.Pipe;

public interface AimSegment {

    void append(Pipe pipe) throws IOException;

    void close() throws IllegalAccessException, IOException;

    long getCount();

    long getSize();

    long getOriginalSize();

    InputStream open(AimFilter filter, String[] columnNames) throws IOException;

    //Integer filter(AimFilter filter, BitSet segmentResult)  throws IOException; 

}
