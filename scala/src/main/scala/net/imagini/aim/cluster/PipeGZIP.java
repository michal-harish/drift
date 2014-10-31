package net.imagini.aim.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.imagini.aim.tools.Pipe;

public class PipeGZIP extends Pipe {


    public PipeGZIP(OutputStream out) throws IOException {
       super(out);
    }
    public PipeGZIP(OutputStream out, Protocol protocol) throws IOException {
        super(out, protocol);
    }
    public PipeGZIP(InputStream in) throws IOException {
       super(in);
    }

    public PipeGZIP(InputStream in, Protocol protocol) throws IOException {
        super(in, protocol);
    }
    public PipeGZIP(Socket socket, Protocol protocol) throws IOException {
        super(socket, protocol);
    }
    @Override protected OutputStream createOutputStreamWrapper(OutputStream out) throws IOException {
        return new GZIPOutputStream(out, true);
    }

    @Override protected InputStream createInputStreamWrapper(InputStream in) throws IOException {
        return new GZIPInputStream(in);
    }


}
