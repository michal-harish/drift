package net.imagini.aim;

import java.io.IOException;

public interface Writable {

    void write(Pipe out) throws IOException;

}
