package net.imagini.aim.types;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import net.imagini.aim.utils.ByteUtils;
import net.imagini.aim.utils.View;

public class AimTypeIPV4 extends AimTypeINT {

    @Override
    public int parse(View value, byte[] dest, int destOffset) {
        int result = 0;
        int component = 0;
        for(int i = value.offset; i<= value.limit; i++) {
            if (value.array[i] == '.') {
                result <<= 8;
                result += component;
                component = 0;
            } else if (value.array[i] < 48 || value.array[i] > 57) {
                throw new IllegalArgumentException("Invalid ipv4 character " +  (char)value.array[i]);
            } else {
                component *= 10;
                component += (value.array[i] - 48) ;
            }
        }
        result <<= 8;
        result += component;
        ByteUtils.putIntValue(result, dest, destOffset);
        return 4;
    }

    @Override
    public String asString(byte[] src, int offset) {
        try {
            return InetAddress
                    .getByAddress(
                            Arrays.copyOfRange(src, offset, offset + 4)).getHostAddress();
        } catch (UnknownHostException e) {
            return null;
        }
    }

    @Override public String escape(String value) { 
        return "'" + value +"'"; 
    }

}