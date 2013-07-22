package net.imagini.aim;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.commons.io.EndianUtils;

public class Pipe {

    private Socket socket;
    private OutputStream outputPipe;
    private InputStream inputPipe;

    public Pipe() {}

    public Pipe(OutputStream out) throws IOException {
        outputPipe = getOutputPipe(out);
    }
    
    public Pipe(InputStream in) throws IOException {
        inputPipe = getInputPipe(in);
    }

    protected InputStream getInputPipe(InputStream in) throws IOException {
        return in;
    }

    protected OutputStream getOutputPipe(OutputStream out) throws IOException {
        return new DataOutputStream(out);
    }

    final public void close() throws IOException {
        if (inputPipe != null) inputPipe.close();
        if (outputPipe != null) outputPipe.close();
        if (socket != null) socket.close();
    }

    final public void write(boolean value) throws IOException {
        outputPipe.write(value ? 1 : 0);
    }

    final public void write(byte value) throws IOException {
        outputPipe.write((int) value);
    }
    final public void write(int value) throws IOException {
        EndianUtils.writeSwappedInteger(outputPipe,value);
    }

    final public void write(long value) throws IOException {
        EndianUtils.writeSwappedLong(outputPipe,value);
    }

    final public void write(byte[] bytes) throws IOException {
        outputPipe.write(bytes);
    }

    final public void write(String value) throws IOException {
        if (value == null) {
            write((int) 0);
        } else {
            byte[] data = value.getBytes();
            write(data.length);
            outputPipe.write(data);
        }
    }
    
    private int readSize(AimDataType type) throws IOException {
        if (type.equals(Aim.STRING)) {
            return EndianUtils.readSwappedInteger(readByteArray(4),0);
        } else if (type instanceof Aim.BYTEARRAY) {
            return ((Aim.BYTEARRAY)type).size;
        } else {
            return type.getSize();
        }
    }

    public byte[] read(AimDataType type) throws IOException {
        return readByteArray(readSize(type));
    }

    final protected byte[] readByteArray(int fixedLen) throws IOException {
        byte[] data = new byte[fixedLen];
        int totalRead = 0;
        while (totalRead < fixedLen) {
            int read = inputPipe.read(data,totalRead,fixedLen-totalRead);
            if (read < 0 ) throw new EOFException();
            else totalRead += read;
        }
        return data;
    }

    public void skip(AimDataType type) throws IOException {
        int skipLen = readSize(type);
        long totalSkipped = 0;
        while (totalSkipped < skipLen) {
            long skipped = inputPipe.skip(skipLen-totalSkipped);
            totalSkipped += skipped;
        }
    }

}
