package net.imagini.aim;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.BitSet;

import org.apache.commons.io.EndianUtils;

public class AimFilter {

    private InputStream range;
    private AimDataType type;

    public AimFilter(InputStream range, AimDataType type) {
        this.range = range;
        this.type = type;
    }

    /**
     * Zero-copy filter routine which scans the column for any
     * match against a set of values..effectively val1 OR val2 OR ...
     * @throws IOException 
     */
    public BitSet any(String... values) throws IOException {
        byte[][] vals = new byte[values.length][]; 
        int i = 0;for(String value:values) {
            vals[i++] = Aim.convert(type, value);
        }
        BitSet result = new BitSet(); 
        int len;
        byte[] int_buf = new byte[4];
        int record = 0;
        try {
            while (true) {
                if (type.equals(Aim.STRING)) {
                    read(range, 4, int_buf);
                    len = EndianUtils.readSwappedInteger(int_buf,0);
                } else {
                    len = type.getSize();
                }
                result.set(record, matchAny(range, len, vals));
                record++;
            }
        } catch (EOFException e) {}
        return result;
    }

    private boolean matchAny(InputStream in, int len, byte[][] vals) throws IOException {
        int[] result = new int[vals.length];
        boolean match = false;
        Arrays.fill(result, 0);
        int i = 0;
        while (i<len) {
            int b = in.read();
            int r = -1;
            match = false;
            for(byte[] val: vals) if (result[++r] == 0) {
                if (len < val.length) {
                    result[r] = -1;
                } else if (len > val.length) {
                    result[r] = 1;
                } else if (b == -1 || b < val[i]) {
                    result[r] = -1;
                } else if (b > val[i]) {
                    result[r] = 1;
                } else {
                    match = true;
                }
            }
            i++;
            if (!match) {
                break;
            }
        }
        return match;
    }

    private void read(InputStream in, int fixedLen, byte[] buf) throws IOException {
        int totalRead = 0;
        while (totalRead < fixedLen) {
            int read = in.read(buf,totalRead,fixedLen-totalRead);
            if (read < 0 ) throw new EOFException();
            else totalRead += read;
        }
    }


}
