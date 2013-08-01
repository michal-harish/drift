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

    Long count(AimFilter filter)  throws IOException;

    InputStream open(AimFilter filter, String[] columnNames) throws IOException;

}
