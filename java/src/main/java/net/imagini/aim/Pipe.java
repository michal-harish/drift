package net.imagini.aim;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import net.imagini.aim.Aim.BYTEARRAY;

import org.apache.commons.io.EndianUtils;

public class Pipe {

    /**
     * We need to use BIG_ENDIAN because the pro(s) are favourable 
     * to our usecase, e.g. lot of streaming filtering.
     */
    private static ByteOrder endian = ByteOrder.BIG_ENDIAN;

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
        write(outputPipe, value);
    }
    final public void write(long value) throws IOException {
        write(outputPipe, value);
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


    public byte[] read(AimDataType type) throws IOException {
        return read(inputPipe, type);
    }

    static public byte[] read(InputStream in, AimDataType type) throws IOException {
        return readByteArray(in, readSize(in, type));
    }

    static public void write(OutputStream out, int v) throws IOException {
        if (endian.equals(ByteOrder.LITTLE_ENDIAN)) {
            EndianUtils.writeSwappedInteger(out,v);
        } else {
            out.write((v >>> 24) & 0xFF);
            out.write((v >>> 16) & 0xFF);
            out.write((v >>>  8) & 0xFF);
            out.write((v >>>  0) & 0xFF);
        }
    }

    static public void write(OutputStream out, long v) throws IOException {
        if (endian.equals(ByteOrder.LITTLE_ENDIAN)) {
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
            return readInteger(readByteArray(in, 4));
        } else if (type instanceof Aim.BYTEARRAY) {
            return ((Aim.BYTEARRAY)type).size;
        } else {
            return type.getSize();
        }
    }

    static public int readInteger(byte[] value) {
        //ByteBuffer bb = ByteBuffer.wrap(value);
        //bb.order(endian);
        //return bb.getInt();
/**/
        if (endian.equals(ByteOrder.LITTLE_ENDIAN)) {
            return EndianUtils.readSwappedInteger(value,0);
        } else {
            return (
                (((int)value[0]) << 24) + 
                (((int)value[1] & 0xff) << 16) + 
                (((int)value[2] & 0xff) << 8) + 
                (((int)value[3] & 0xff) << 0)
            );
        }/**/
    }

    static public long readLong(byte[] value, int o) {
        if (endian.equals(ByteOrder.LITTLE_ENDIAN)) {
            return EndianUtils.readSwappedLong(value, o);
        } else {
            return (((long)value[o+0] << 56) +
                    (((long)value[o+1] & 0xff) << 48) +
                    (((long)value[o+2] & 0xff) << 40) +
                    (((long)value[o+3] & 0xff) << 32) +
                    (((long)value[o+4] & 0xff) << 24) +
                    (((long)value[o+5] & 0xff) << 16) +
                    (((long)value[o+6] & 0xff) <<  8) +
                    (((long)value[o+7] & 0xff) <<  0));
        }
    }

    static public byte[] readByteArray(InputStream in, int fixedLen) throws IOException {
        byte[] data = new byte[fixedLen];
        int totalRead = 0;
        while (totalRead < fixedLen) {
            int read = in.read(data,totalRead,fixedLen-totalRead);
            if (read < 0 ) throw new EOFException();
            else totalRead += read;
        }
        return data;
    }

    static public void skip(InputStream in, AimDataType type) throws IOException {
        int skipLen = readSize(in, type);
        long totalSkipped = 0;
        while (totalSkipped < skipLen) {
            long skipped = in.skip(skipLen-totalSkipped);
            totalSkipped += skipped;
        }
    }

    static public String convert(AimDataType type, byte[] value) {
        if (type.equals(Aim.BOOL)) {
            return String.valueOf(value[0]>0);
        } else if (type.equals(Aim.BYTE)) {
            return String.valueOf(value[0]);
        } else if (type.equals(Aim.INT)) {
            return String.valueOf(readInteger(value));
        } else if (type.equals(Aim.LONG)) {
            return String.valueOf(readLong(value,0));
        } else if (type.equals(Aim.STRING)) {
            return new String(value);
        } else if (type instanceof Aim.BYTEARRAY) {
            return new String(value);
        } else {
            throw new IllegalArgumentException("Unknown column.type " + type);
        }
    }
    static public byte[] convert(AimDataType type, String value) {
        ByteBuffer bb;
        if (type.equals(Aim.BOOL)) {
            bb = ByteBuffer.allocate(1);
            bb.put((byte) (Boolean.valueOf(value) ? 1 : 0));
        } else if (type.equals(Aim.BYTE)) {
            bb = ByteBuffer.allocate(1);
            bb.put(Byte.valueOf(value));
        } else if (type.equals(Aim.INT)) {
            bb = ByteBuffer.allocate(4);
            bb.order(endian);
            bb.putInt(Integer.valueOf(value));
        } else if (type.equals(Aim.LONG)) {
            bb = ByteBuffer.allocate(8);
            bb.order(endian);
            bb.putLong(Long.valueOf(value));
        } else if (type.equals(Aim.STRING)) {
            bb = ByteBuffer.allocate(value.length());
            bb.put(value.getBytes());
        } else if (type instanceof BYTEARRAY) {
            int size = ((BYTEARRAY)type).size;
            if (value.length() != size) {
                throw new IllegalArgumentException("Invalid value for fixed-length byte array column; required size: " + size);
            }
            bb = ByteBuffer.allocate(size);
            bb.order(endian);
            bb.put(value.getBytes());
        } else {
            throw new IllegalArgumentException("Unknown data type " + type.getClass().getSimpleName());
        }
        return bb.array();
    }
/*
    static public int readZeroCopy(InputStream in, AimDataType type, byte[] buf) throws IOException {
        int len;
        if (type.equals(Aim.STRING)) {
            readByteArrayZeroCopy(in, 4, buf);
            len = readInteger(buf);
        } else if (type instanceof Aim.BYTEARRAY) {
            len = ((Aim.BYTEARRAY)type).size;
        } else {
            len = type.getSize();
        }
        readByteArrayZeroCopy(in, len, buf);
        return len;
    }

    static public void readByteArrayZeroCopy(InputStream in, int fixedLen, byte[] buf) throws IOException {
        int totalRead = 0;
        while (totalRead < fixedLen) {
            int read = in.read(buf,totalRead,fixedLen-totalRead);
            if (read < 0 ) throw new EOFException();
            else totalRead += read;
        }
    }*/
}
