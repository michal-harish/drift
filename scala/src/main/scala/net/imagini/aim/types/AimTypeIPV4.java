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
    public String asString(View view) {
        try {
            return InetAddress
                    .getByAddress(
                            Arrays.copyOfRange(view.array, view.offset,
                                    view.offset + 4)).getHostAddress();
        } catch (UnknownHostException e) {
            return null;
        }
    }

    @Override
    public int convert(String value, byte[] dest, int destOffset) {
        try {
            return ByteUtils.copy(InetAddress.getByName(value).getAddress(), 0,
                    dest, destOffset, 4);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }

    }

    @Override
    public byte[] convert(String value) {
        try {
            return InetAddress.getByName(value).getAddress();
        } catch (UnknownHostException e) {
            byte[] result = new byte[4];
            Arrays.fill(result, (byte) 0);
            return result;
        }

    }

    @Override
    public String convert(byte[] value) {
        try {
            return InetAddress.getByAddress(value).getHostAddress();
        } catch (UnknownHostException e) {
            return null;
        }
    }

}