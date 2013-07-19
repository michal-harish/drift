package net.imagini.aim;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class AimPipe {

    private DataOutput outputPipe;
    private Closeable resource;

    public AimPipe(InetAddress host, int port) throws IOException {
        Socket socket = new Socket(host, port);
        outputPipe = new DataOutputStream(socket.getOutputStream());
        resource = socket;
    }
    public AimPipe(String name, boolean write) throws IOException {
        RandomAccessFile pipe = new RandomAccessFile("\\\\.\\pipe\\" + name, write ? "rw" : "r");
        outputPipe = pipe;
        resource = pipe;
    }

    public void close() throws IOException {
        resource.close();
    }

    public void write(boolean value) throws IOException {
        outputPipe.write(value ? 1 : 0);
    }
    public static boolean readBool(DataInput in) throws IOException {
        return  in.readByte() > 0 ? true : false;
    }

    public void write(byte value) throws IOException {
        outputPipe.writeByte((int) value);
    }
    public static byte readByte(DataInput in) throws IOException {
        return in.readByte();
    }

    public void write(int value) throws IOException {
        /**/
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(value);
        outputPipe.write(bb.array(), 0, 4);
        /**/
        //outputPipe.writeInt(value);
    }
    public static int readInt32(DataInput in) throws IOException {
        /**/
        ByteBuffer bb = ByteBuffer.allocate(4);
        in.readFully(bb.array());
        bb.order(ByteOrder.LITTLE_ENDIAN);
        return bb.getInt();
        /**/
        //return in.readInt();
    }

    public void write(long value) throws IOException {
        outputPipe.writeLong(value);
    }
    public static long readLong(DataInput in) throws IOException {
        return in.readLong();
    }

    public void write(byte[] bytes) throws IOException {
        outputPipe.write(bytes);
    }
    public static byte[] readByteArray(DataInput in, int fixedLen) throws IOException {
        byte[] data = new byte[fixedLen];
        in.readFully(data);
        return data;
    }

    public void write(String value) throws IOException {
        byte[] data = value.getBytes();
        write(data.length);
        outputPipe.write(data);
    }

    public static String readString(DataInput in) throws IOException {
        int len = readInt32(in);
        byte[] data = new byte[len];
        in.readFully(data);
        return new String(data,0,len);
    }
}
