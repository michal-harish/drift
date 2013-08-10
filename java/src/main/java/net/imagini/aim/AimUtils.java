package net.imagini.aim;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedHashMap;

import joptsimple.internal.Strings;

import net.imagini.aim.AimTypeAbstract.AimDataType;
import net.imagini.aim.pipes.Pipe;

import org.apache.commons.io.EndianUtils;


public class AimUtils {

    public static AimSchema parseSchema(String declaration) {
        final String[] dec = declaration.split(",");
        LinkedHashMap<String, AimType> result = new LinkedHashMap<String,AimType>();
        for(int f=0; f<dec.length; f++) {
            String name = String.valueOf(f+1);
            String function = null;
            Integer length = null;
            String type = dec[f].trim();
            if (type.contains("(")) {
                name = type.substring(0, type.indexOf("("));
                type = type.substring(type.indexOf("(")+1,type.indexOf(")"));
            }
            type = type.toUpperCase();
            if (type.contains(":")) {
                function = type.split(":")[0];
                type = type.split(":")[1];
            }
            if (type.contains("[")) {
                length = Integer.valueOf(type.substring(type.indexOf("[")+1,type.indexOf("]")));
                type = type.substring(0, type.indexOf("["));
            }
            switch(type) {
                case "BOOL": result.put(name,Aim.BOOL); break;
                case "BYTE": result.put(name,Aim.BYTE); break;
                case "INT": result.put(name,Aim.INT); break;
                case "LONG": result.put(name,Aim.LONG); break;
                case "BYTEARRAY": result.put(name,Aim.BYTEARRAY(length)); break;
                case "STRING": result.put(name,Aim.STRING); break;
                default: throw new IllegalArgumentException("Unknown data type " + type);
            }
            if (function != null) {
                switch(function) {
                    case "UUID": result.put(name, Aim.UUID(result.get(name).getDataType())); break;
                    case "IPV4": result.put(name, Aim.IPV4(result.get(name).getDataType())); break;
                    default: throw new IllegalArgumentException("Unknown type " + type);
                }
            }
        }
        return new AimSchema(result);
    }

    public static String[] collect(final AimSchema schema, final Pipe in) throws IOException {
        if (in == null) return null;
        String[] result = new String[schema.size()];
        int i = 0; for(AimType type: schema.def()) {
            byte[] value = in.read(type.getDataType());
            result[i++] = type.convert(value);
        }
        return result;
    }

    public static long skip(InputStream in, AimDataType type) throws IOException {
        int size;
        if (type.equals(Aim.STRING)) {
            byte[] buf = new byte[4];
            read(in, buf, 0, 4);
            size = getIntegerValue(buf);
        } else {
            size = type.getSize();
        }
        return in.skip(size);
    }

    public static long skip(ByteBuffer in, AimDataType type) {
        int size = size(in, type);
        in.position(in.position() + size);
        return size;
    }

    public static int size(ByteBuffer in, AimDataType type) {
        int size;
        if (type.equals(Aim.STRING)) {
            size = getIntegerValue(in) + 4;
        } else {
            size = type.getSize();
        }
        return size;
    }

    public static String read(InputStream in) throws IOException {
        byte[] buf = new byte[Aim.COLUMN_BUFFER_SIZE];
        read(in,Aim.STRING,buf);
        return Aim.STRING.convert(buf);
    }

    static public int read(InputStream in,AimDataType type, byte[] buf) throws IOException {
        int size;
        int offset = 0;
        if (type.equals(Aim.STRING)) {
            read(in, buf, 0, (offset  = 4));
            size = getIntegerValue(buf);
        } else {
            size = type.getSize();
        }
        read(in, buf, offset, size);
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
        //TODO array buffer specific code should be done with slice instead
        int o = dest.arrayOffset();
        o += dest.position();
        src.get(dest.array(),o,size);
        dest.position(dest.position()+size);
        return head + size;
    }


    public static int write(AimDataType type, byte[] value, OutputStream out) throws IOException {
        int size = 0;
        if (type.equals(Aim.STRING)) {
            size = getIntegerValue(value);
            out.write(value, 0, size + 4);
            return size + 4;
        } else {
            size = type.getSize();
        }
        out.write(value,0,size);
        return size;
    }

    public static int write(AimDataType type, byte[] value, ByteBuffer out) throws IOException {
        int size = 0;
        if (type.equals(Aim.STRING)) {
            size = getIntegerValue(value);
            out.put(value,0, size+4);
            return size + 4;
        } else {
            size = type.getSize();
        }
        out.put(value,0,size);
        return size;
    }

    static public void read(InputStream in, ByteBuffer buf, int len) throws IOException {
        int totalRead = 0;
        while (totalRead < len) {
            int read = in.read(buf.array(),buf.arrayOffset()+buf.position(),len-totalRead);
            if (read < 0 ) throw new EOFException();
            else {
                buf.position(buf.position()+read);
                totalRead += read;
            }
        }
    }

    static public void read(InputStream in, byte[] buf, int offset, int len) throws IOException {
        int totalRead = 0;
        while (totalRead < len) {
            int read = in.read(buf,offset+totalRead,len-totalRead);
            if (read < 0 ) throw new EOFException();
            else totalRead += read;
        }
    }

    static public int getIntegerValue(byte[] value) {
        return getIntegerValue(value, 0);
    }

    static public int getIntegerValue(byte[] value, int offset) {
        if (Aim.endian.equals(ByteOrder.LITTLE_ENDIAN)) {
            return EndianUtils.readSwappedInteger(value,0);
        } else {
            return (
                (((int)value[offset+0]) << 24) + 
                (((int)value[offset+1] & 0xff) << 16) + 
                (((int)value[offset+2] & 0xff) << 8) + 
                (((int)value[offset+3] & 0xff) << 0)
            );
        }
    }

    static public int getIntegerValue(ByteBuffer value) {
        int offset = value.position();
        if (Aim.endian.equals(ByteOrder.LITTLE_ENDIAN)) {
            return (
                    (((int)value.get(offset+3)) << 24) + 
                    (((int)value.get(offset+2) & 0xff) << 16) + 
                    (((int)value.get(offset+1) & 0xff) << 8) + 
                    (((int)value.get(offset+0) & 0xff) << 0)
                );
        } else {
            return (
                (((int)value.get(offset+0)) << 24) + 
                (((int)value.get(offset+1) & 0xff) << 16) + 
                (((int)value.get(offset+2) & 0xff) << 8) + 
                (((int)value.get(offset+3) & 0xff) << 0)
            );
        }
    }

    static public long getLongValue(byte[] value, int o) {
        if (Aim.endian.equals(ByteOrder.LITTLE_ENDIAN)) {
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

    public static void putIntegerValue(int value, byte[] result, int offset) {
        if (Aim.endian.equals(ByteOrder.LITTLE_ENDIAN)) {
            EndianUtils.writeSwappedInteger(result,offset,value);
        } else {
            result[offset+0] = (byte)((value >>> 24) & 0xFF);
            result[offset+1] = (byte)((value >>> 16) & 0xFF);
            result[offset+2] = (byte)((value >>>  8) & 0xFF);
            result[offset+3] = (byte)((value >>>  0) & 0xFF);
        }
    }

    public static String exceptionAsString(Exception e) {
        String[] trace = new String[e.getStackTrace().length];
        int i = 0; for(StackTraceElement t:e.getStackTrace()) trace[i++] = t.toString();
        return e.toString() + "\n"  + Strings.join(trace,"\n");
    }

}
