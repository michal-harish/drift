package net.imagini.aim.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import net.imagini.aim.types.Aim;
import net.imagini.aim.types.AimDataType;
import net.imagini.aim.types.AimTypeBYTEARRAY;
import net.imagini.aim.utils.ByteUtils;

public class PipeUtils {

    public static int write(AimDataType type, byte[] value, OutputStream out)
            throws IOException {
        int size = 0;
        if (type.equals(Aim.STRING)) {
            size = ByteUtils.getIntValue(value);
            out.write(value, 0, size + 4);
            return size + 4;
        } else {
            size = type.getSize();
        }
        out.write(value, 0, size);
        return size;
    }

    static private int readSize(InputStream in, AimDataType type) throws IOException {
        if (type.equals(Aim.STRING)) {
            byte[] b = new byte[4];
            ByteUtils.read(in,b,0,4);
            return ByteUtils.getIntValue(b);
        } else if (type instanceof AimTypeBYTEARRAY) {
            return ((AimTypeBYTEARRAY)type).size;
        } else {
            return type.getSize();
        }
    }

    static public byte[] read(InputStream in, AimDataType type) throws IOException {
        int size = readSize(in, type);
        byte[] result;
        if (type.equals(Aim.STRING)) {
            result = new byte[size + 4];
            ByteUtils.putIntValue(size,result,0);
            ByteUtils.read( in, result, 4 , size);
        } else {
            result = new byte[size];
            ByteUtils.read( in, result, 0 , size);
        }
        return result;
    }

    static public void skip(InputStream in, AimDataType type) throws IOException {
        int skipLen = readSize(in, type);
        long totalSkipped = 0;
        while (totalSkipped < skipLen) {
            long skipped = in.skip(skipLen-totalSkipped);
            totalSkipped += skipped;
        }
    }

    private static byte[] intBuffer = new byte[4];

    // FIXME sizeOf is non-thread-safe because of intBuffer;
    public static int sizeOf(InputStream in, AimDataType type)
            throws IOException {
        int size;
        if (type.equals(Aim.STRING)) {
            ByteUtils.read(in, intBuffer, 0, 4);
            size = ByteUtils.getIntValue(intBuffer);
        } else {
            size = type.getSize();
        }
        return size;
    }

    static public int read(InputStream in, AimDataType type, byte[] buf)
            throws IOException {
        int size;
        int offset = 0;
        if (type.equals(Aim.STRING)) {
            ByteUtils.read(in, buf, 0, (offset = 4));
            size = ByteUtils.getIntValue(buf);
        } else {
            size = type.getSize();
        }
        ByteUtils.read(in, buf, offset, size);
        return offset + size;
    }

    public static long copy(ByteBuffer src, AimDataType type, ByteBuffer dest) {
        int size;
        int head = 0;
        if (type.equals(Aim.STRING)) {
            size = src.getInt();
            dest.putInt(size);
            head = 4;
        } else {
            size = type.getSize();
        }
        // FIXME array buffer specific code should be done with slice instead
        int o = dest.arrayOffset();
        o += dest.position();
        src.get(dest.array(), o, size);
        dest.position(dest.position() + size);
        return head + size;
    }

    public static int sizeOf(ByteBuffer in, AimDataType type) {
        int size;
        if (type.equals(Aim.STRING)) {
            size = ByteUtils.getIntValue(in) + 4;
        } else {
            size = type.getSize();
        }
        return size;
    }

    static public int write(AimDataType type, byte[] value, ByteBuffer out)
            throws IOException {
        int size = 0;
        if (type.equals(Aim.STRING)) {
            size = ByteUtils.getIntValue(value);
            out.put(value, 0, size + 4);
            return size + 4;
        } else {
            size = type.getSize();
        }
        out.put(value, 0, size);
        return size;
    }

}
