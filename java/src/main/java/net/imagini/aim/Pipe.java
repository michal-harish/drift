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


    final public boolean readBool() throws IOException {
        byte[] bb = readByteArray(1);
        boolean result = bb[0] > 0 ? true : false;
        //System.out.println("bool: " + result);
        return result;
    }

    final public byte readByte() throws IOException {
        byte[] bb = readByteArray(1);
        //System.out.println("byte: " + bb[0]);
        return bb[0];
    }

    final public int readInt32() throws IOException {
        byte[] bb = readByteArray(4);
        int result = EndianUtils.readSwappedInteger(bb,0);
        //System.out.println("int32: " + result);
        return result;
    }

    final public long readLong() throws IOException {
        byte[] bb = readByteArray(8);
        long result = EndianUtils.readSwappedLong(bb,0);
        //System.out.println("long: " + result);
        return result;
    }

    final public byte[] readByteArray(int fixedLen) throws IOException {
        byte[] data = new byte[fixedLen];
        int totalRead = 0;
        while (totalRead < fixedLen) {
            int read = inputPipe.read(data,totalRead,fixedLen-totalRead);
            if (read < 0 ) throw new EOFException();
            else totalRead += read;
        }
        return data;
    }

    final public String readString() throws IOException {
        int len = readInt32();
        byte[] data = readByteArray(len);
        String result = new String(data,0,len);
        //System.out.println("string: " + result);
        return result;
    }
}
