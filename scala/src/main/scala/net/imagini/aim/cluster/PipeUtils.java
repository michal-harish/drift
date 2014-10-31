package net.imagini.aim.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import net.imagini.aim.types.Aim;
import net.imagini.aim.types.AimDataType;
import net.imagini.aim.types.AimTypeBYTEARRAY;
import net.imagini.aim.utils.ByteUtils;

public class PipeUtils {

    public static int write(AimDataType type, ByteBuffer value, OutputStream out)
            throws IOException {
        byte[] array = value.array();
        int offset = value.arrayOffset() + value.position();
        int size = 0;
        if (type.equals(Aim.STRING)) {
            size = ByteUtils.getIntValue(array, offset) + 4;
        } else {
            size = type.getSize();
        }
        out.write(array, offset, size);
        return size;
    }

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

}
