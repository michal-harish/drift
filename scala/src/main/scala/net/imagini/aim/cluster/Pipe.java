package net.imagini.aim.cluster;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import net.imagini.aim.tools.StreamUtils;
import net.imagini.aim.types.Aim;
import net.imagini.aim.types.AimDataType;

/**
 * Pipe is a bi-directional stream that is aimtype-aware
 * 
 * @author mharis
 */
public class Pipe {

    final public Protocol protocol;
    private OutputStream outputPipe;
    private InputStream inputPipe;

    public Pipe() {
        this.protocol = Protocol.RESERVED;
    }

    public Pipe(Socket socket, Protocol protocol) throws IOException {
        this(socket.getOutputStream(), protocol);
        inputPipe = createInputStreamWrapper(socket.getInputStream());
    }

    public Pipe(OutputStream out) throws IOException {
        outputPipe = createOutputStreamWrapper(out);
        this.protocol = Protocol.RESERVED;
    }

    public Pipe(OutputStream out, Protocol protocol) throws IOException {
        byte compression;
        if (getClass().equals(Pipe.class))
            compression = 0;
        else if (getClass().equals(PipeLZ4.class))
            compression = 1;
        else if (getClass().equals(PipeGZIP.class))
            compression = 2;
        else
            throw new IOException("Unsupported pipe type "
                    + getClass().getSimpleName());
        StreamUtils.writeInt(out, "DRIFT".hashCode());
        out.write(compression);
        StreamUtils.writeInt(out, protocol.id);
        this.protocol = protocol;
        outputPipe = createOutputStreamWrapper(out);
    }

    public Pipe(InputStream in) throws IOException {
        inputPipe = createInputStreamWrapper(in);
        this.protocol = Protocol.RESERVED;
    }

    public Pipe(InputStream in, Protocol protocol) throws IOException {
        inputPipe = createInputStreamWrapper(in);
        this.protocol = protocol;
    }

    static public Pipe open(Socket socket) throws IOException {
        Pipe pipe = open(socket.getInputStream());
        pipe.outputPipe = pipe.createOutputStreamWrapper(socket
                .getOutputStream());
        return pipe;
    }

    static public Pipe open(InputStream in) throws IOException {
        int signature = StreamUtils.readInt(in);
        if (signature != "DRIFT".hashCode()) {
            throw new IOException("Invalid pipe header signature");
        }
        byte compression = (byte) in.read();
        int pipe_protocol = StreamUtils.readInt(in);
        Protocol protocol = Protocol.get(pipe_protocol);
        if (protocol == null)
            throw new IOException("Unknown protocol id " + pipe_protocol);
        switch (compression) {
        case 0:
            return new Pipe(in, protocol);
        case 1:
            return new PipeLZ4(in, protocol);
        case 2:
            return new PipeGZIP(in, protocol);
        default:
            throw new IOException("Invalid pipe header type");
        }
    }

    protected InputStream createInputStreamWrapper(InputStream in)
            throws IOException {
        return in;
    }

    public InputStream getInputStream() {
        return inputPipe;
    }

    public OutputStream getOutputStream() {
        return outputPipe;
    }

    protected OutputStream createOutputStreamWrapper(OutputStream out)
            throws IOException {
        return new DataOutputStream(out);
    }

    final public void flush() throws IOException {
        outputPipe.flush();
    }

    final public void close() throws IOException {
        if (inputPipe != null)
            inputPipe.close();
        inputPipe = null;
        outputPipe = null;
    }

    final public void writeInt(int value) throws IOException {
        StreamUtils.writeInt(outputPipe, value);
    }

    final public Pipe write(String value) throws IOException {
        StreamUtils.write(Aim.STRING, Aim.STRING.convert(value), outputPipe);
        return this;
    }

    public int write(AimDataType type, ByteBuffer value) throws IOException {
        return StreamUtils.write(type, value, outputPipe);
    }

    public int write(AimDataType type, byte[] value) throws IOException {
        return StreamUtils.write(type, value, outputPipe);
    }

    public int readInto(AimDataType type, ByteBuffer buf) throws IOException {
        return StreamUtils.read(inputPipe, type, buf);
    }

    public String read() throws IOException {
        return Aim.STRING.convert(read(Aim.STRING));
    }

    public int readInt() throws IOException {
        return StreamUtils.readInt(inputPipe);
    }

    public byte[] read(AimDataType type) throws IOException {
        return StreamUtils.read(inputPipe, type);
    }

    public void skip(AimDataType type) throws IOException {
        StreamUtils.skip(inputPipe, type);
    }

}
