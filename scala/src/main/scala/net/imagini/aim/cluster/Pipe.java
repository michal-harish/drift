package net.imagini.aim.cluster;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;

import net.imagini.aim.types.Aim;
import net.imagini.aim.types.AimDataType;
import net.imagini.aim.utils.ByteUtils;

/**
 * Pipe is a bi-directional stream that is aimtype-aware
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
        int pipe_type;
        if (getClass().equals(Pipe.class)) pipe_type = 0;
        else if (getClass().equals(PipeLZ4.class)) pipe_type = 1;
        else if (getClass().equals(PipeGZIP.class)) pipe_type = 2;
        else throw new IOException("Unsupported pipe type " + getClass().getSimpleName());
        out.write("AIM".getBytes());
        out.write(pipe_type);
        ByteUtils.write(out, protocol.id);
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
        pipe.outputPipe = pipe.createOutputStreamWrapper(socket.getOutputStream());
        return pipe;
    }
    static public Pipe open(InputStream in) throws IOException {
        byte[] sig = new byte[3];
        ByteUtils.read(in, sig, 0, 3);
        if (!Arrays.equals("AIM".getBytes(),sig)) {
            throw new IOException("Invalid pipe header signature");
        }
        byte[] pipe_type = new byte[1];
        ByteUtils.read(in, pipe_type, 0, 1);
        byte[] proto = new byte[4];
        ByteUtils.read(in, proto, 0, 4);
        int pipe_protocol = ByteUtils.getIntValue(proto);
        Protocol protocol = Protocol.get(pipe_protocol);
        if (protocol == null) throw new IOException("Unknown protocol id " + pipe_protocol);
        switch(pipe_type[0]) {
            case 0: return new Pipe(in,protocol);
            case 1: return new PipeLZ4(in,protocol);
            case 2: return new PipeGZIP(in,protocol);
            default: throw new IOException("Invalid pipe header type");
        }
    }
    protected InputStream createInputStreamWrapper(InputStream in) throws IOException {
        return in;
    }

    public InputStream getInputStream() {
        return inputPipe;
    }

    protected OutputStream createOutputStreamWrapper(OutputStream out) throws IOException {
        return new DataOutputStream(out);
    }

    final public void flush() throws IOException {
        outputPipe.flush();
    }
    final public void close() throws IOException {
        if (inputPipe != null) inputPipe.close();
        inputPipe = null;
        outputPipe = null;
    }

    final public void write(boolean value) throws IOException {
        outputPipe.write(value ? 1 : 0);
    }

    final public void write(byte value) throws IOException {
        outputPipe.write((int) value);
    }
    final public void write(int value) throws IOException {
        ByteUtils.write(outputPipe, value);
    }
    final public void write(long value) throws IOException {
        ByteUtils.write(outputPipe, value);
    }

    final public void write(byte[] bytes) throws IOException {
        outputPipe.write(bytes);
    }

    final public Pipe write(String value) throws IOException {
        if (value == null) {
            write((int) 0);
        } else {
            byte[] data = value.getBytes();
            write(data.length);
            outputPipe.write(data);
        }
        return this;
    }
    public int write(AimDataType type, ByteBuffer value) throws IOException {
        return PipeUtils.write(type, value, outputPipe);
    }
    public int write(AimDataType type, byte[] value) throws IOException {
        return PipeUtils.write(type, value, outputPipe);
    }

    public int read(AimDataType type, ByteBuffer buf) throws IOException {
        int size;
        int offset = 0;
        if (type.equals(Aim.STRING)) {
            buf.mark();
            ByteUtils.read(inputPipe, buf, (offset  = 4));
            buf.reset();
            size = ByteUtils.asIntValue(buf);
            buf.position(buf.position()+4);
        } else {
            size = type.getSize();
        }
        ByteUtils.read(inputPipe, buf, size);
        return offset + size;
    }

    public String read() throws IOException {
        return Aim.STRING.convert(read(Aim.STRING));
    }
    public boolean readBool() throws IOException {
        return read(Aim.BOOL)[0] == 0 ? false : true;
    }
    public byte readByte() throws IOException {
        return (byte)read(Aim.BYTE)[0];
    }
    public Integer readInt() throws IOException {
        return ByteUtils.getIntValue(read(Aim.INT));
    }
    public Long readLong() throws IOException {
        return ByteUtils.getLongValue(read(Aim.LONG),0);
    }

    public byte[] read(AimDataType type) throws IOException {
        return PipeUtils.read(inputPipe, type);
    }

    public void skip(AimDataType type) throws IOException {
        PipeUtils.skip(inputPipe, type);
    }

}
