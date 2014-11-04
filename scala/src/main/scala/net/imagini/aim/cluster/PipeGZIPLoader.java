package net.imagini.aim.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.imagini.aim.tools.StreamUtils;
import net.imagini.aim.types.Aim;
import net.imagini.aim.types.AimDataType;

public class PipeGZIPLoader extends Pipe {

    protected GZIPOutputStream directOutputStream;

    public PipeGZIPLoader(Socket socket, Protocol protocol) throws IOException {
        super(socket, protocol);
    }

    @Override
    protected OutputStream createOutputStreamWrapper(OutputStream out)
            throws IOException {
        this.directOutputStream = new GZIPOutputStream(out, true);
        return out;
    }

    @Override
    protected InputStream createInputStreamWrapper(InputStream in)
            throws IOException {
        return new GZIPInputStream(in);
    }

    public GZIPOutputStream getDirectOutputStream() {
        return directOutputStream;
    }
    public void flushDirect() throws IOException {
        directOutputStream.flush();
        directOutputStream.finish();
    }

    public Pipe writeDirect(String value) throws IOException {
        StreamUtils.write(Aim.STRING, Aim.STRING.convert(value),
                directOutputStream);
        return this;
    }

    public int writeDirect(AimDataType type, byte[] value) throws IOException {
        return StreamUtils.write(type, value, directOutputStream);
    }

}
