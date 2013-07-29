package net.imagini.aim.pipes;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteOrder;

import net.imagini.aim.Aim;
import net.imagini.aim.AimTypeAbstract.AimDataType;
import net.imagini.aim.AimUtils;

import org.apache.commons.io.EndianUtils;

import scala.actors.threadpool.Arrays;

public class Pipe {

    public static enum Protocol {
        BINARY(0),LOADER(1),QUERY(2);
        public final int id;
        private Protocol(int id) { this.id = id; }
        static public Protocol get(int id) { for(Protocol p: Protocol.values()) if (p.id == id) return p; return null;}
    }
    final public Protocol protocol;
    private OutputStream outputPipe;
    private InputStream inputPipe;

    public Pipe() { 
        this.protocol = Pipe.Protocol.BINARY; 
    }
    public Pipe(Socket socket, Protocol protocol) throws IOException {
        this(socket.getOutputStream(), protocol);
        inputPipe = getInputPipe(socket.getInputStream());
    }

    public Pipe(OutputStream out) throws IOException {
        outputPipe = getOutputPipe(out);
        this.protocol = Pipe.Protocol.BINARY;
    }
    public Pipe(OutputStream out, Protocol protocol) throws IOException {
        int pipe_type;
        if (getClass().equals(Pipe.class)) pipe_type = 0;
        else if (getClass().equals(PipeLZ4.class)) pipe_type = 1;
        else if (getClass().equals(PipeGZIP.class)) pipe_type = 2;
        else throw new IOException("Unsupported pipe type " + getClass().getSimpleName());
        out.write("AIM".getBytes());
        out.write(pipe_type);
        write(out, protocol.id);
        this.protocol = protocol;
        outputPipe = getOutputPipe(out);
    }

    public Pipe(InputStream in) throws IOException {
        inputPipe = getInputPipe(in);
        this.protocol = Pipe.Protocol.BINARY;
    }

    public Pipe(InputStream in, Protocol protocol) throws IOException {
        inputPipe = getInputPipe(in);
        this.protocol = protocol;
    }
    static public Pipe open(Socket socket) throws IOException {
        Pipe pipe = open(socket.getInputStream());
        pipe.outputPipe = pipe.getOutputPipe(socket.getOutputStream());
        return pipe;
    }
    static public Pipe open(InputStream in) throws IOException {
        byte[] sig = new byte[3];
        AimUtils.read(in, sig, 0, 3);
        if (!Arrays.equals("AIM".getBytes(),sig)) {
            throw new IOException("Invalid pipe header signature");
        }
        byte[] pipe_type = new byte[1];
        AimUtils.read(in, pipe_type, 0, 1);
        byte[] proto = new byte[4];
        AimUtils.read(in, proto, 0, 4);
        int pipe_protocol = AimUtils.getIntegerValue(proto);
        Protocol protocol = Protocol.get(pipe_protocol);
        if (protocol == null) throw new IOException("Unknown protocol id " + pipe_protocol);
        switch(pipe_type[0]) {
            case 0: return new Pipe(in,protocol);
            case 1: return new PipeLZ4(in,protocol);
            case 2: return new PipeGZIP(in,protocol);
            default: throw new IOException("Invalid pipe header type");
        }
    }
    protected InputStream getInputPipe(InputStream in) throws IOException {
        return in;
    }

    protected OutputStream getOutputPipe(OutputStream out) throws IOException {
        return new DataOutputStream(out);
    }

    final public void flush() throws IOException {
        outputPipe.flush();
    }
    final public void close() throws IOException {
        if (inputPipe != null) inputPipe.close();
        inputPipe = null;
        if (outputPipe != null) outputPipe.close();
        outputPipe = null;
    }

    final public void write(boolean value) throws IOException {
        outputPipe.write(value ? 1 : 0);
    }

    final public void write(byte value) throws IOException {
        outputPipe.write((int) value);
    }
    final public void write(int value) throws IOException {
        write(outputPipe, value);
    }
    final public void write(long value) throws IOException {
        write(outputPipe, value);
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
    public int write(AimDataType type, byte[] value) throws IOException {
        return AimUtils.write(type, value, outputPipe);
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
        return AimUtils.getIntegerValue(read(Aim.INT));
    }
    public Long readLong() throws IOException {
        return AimUtils.getLongValue(read(Aim.LONG),0);
    }

    public byte[] read(AimDataType type) throws IOException {
        return read(inputPipe, type);
    }

    public void skip(AimDataType type) throws IOException {
        skip(inputPipe, type);
    }

    static public byte[] read(InputStream in, AimDataType type) throws IOException {
        int size = readSize(in, type);
        byte[] result;
        if (type.equals(Aim.STRING)) {
            result = new byte[size + 4];
            AimUtils.putIntegerValue(size,result,0);
            AimUtils.read( in, result, 4 , size);
        } else {
            result = new byte[size];
            AimUtils.read( in, result, 0 , size);
        }
        return result;
    }
    static public void write(OutputStream out, int v) throws IOException {
        if (Aim.endian.equals(ByteOrder.LITTLE_ENDIAN)) {
            EndianUtils.writeSwappedInteger(out,v);
        } else {
            out.write((v >>> 24) & 0xFF);
            out.write((v >>> 16) & 0xFF);
            out.write((v >>>  8) & 0xFF);
            out.write((v >>>  0) & 0xFF);
        }
    }

    static public void write(OutputStream out, long v) throws IOException {
        if (Aim.endian.equals(ByteOrder.LITTLE_ENDIAN)) {
            EndianUtils.writeSwappedLong(out,v);
        } else {
            out.write((byte)(v >>> 56));
            out.write((byte)(v >>> 48));
            out.write((byte)(v >>> 40));
            out.write((byte)(v >>> 32));
            out.write((byte)(v >>> 24));
            out.write((byte)(v >>> 16));
            out.write((byte)(v >>>  8));
            out.write((byte)(v >>>  0));
        }
    }

    static private int readSize(InputStream in, AimDataType type) throws IOException {
        if (type.equals(Aim.STRING)) {
            byte[] b = new byte[4];
            AimUtils.read(in,b,0,4);
            return AimUtils.getIntegerValue(b);
        } else if (type instanceof Aim.BYTEARRAY) {
            return ((Aim.BYTEARRAY)type).size;
        } else {
            return type.getSize();
        }
    }

    static public void skip(InputStream in, AimDataType type) throws IOException {
        int skipLen = readSize(in, type);
        long totalSkipped = 0;
        while (totalSkipped < skipLen) {
            long skipped = in.skip(skipLen-totalSkipped);
            totalSkipped += skipped;
        }
    }
}
