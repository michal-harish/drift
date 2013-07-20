package net.imagini.aim;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class PipeGZIP extends Pipe {


    public PipeGZIP(OutputStream out) throws IOException {
       super(out);
    }

    public PipeGZIP(InputStream in) throws IOException {
       super(in);
    }

    @Override protected OutputStream getOutputPipe(OutputStream out) throws IOException {
        return new GZIPOutputStream(out, true);
    }

    @Override protected InputStream getInputPipe(InputStream in) throws IOException {
        return new GZIPInputStream(in);
    }


}
