package net.imagini.aim;

import java.util.Arrays;

public class ByteArrayWrapper implements Comparable<ByteArrayWrapper> {
    public final byte[] bytes;
    private int classifier;

    public ByteArrayWrapper(byte[] data, int classifier) {
        if (data == null) {
            throw new NullPointerException();
        }
        this.classifier = classifier;
        this.bytes = data;
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof ByteArrayWrapper)) {
            return false;
        }
        return Arrays.equals(bytes, ((ByteArrayWrapper)other).bytes);
    }

    @Override public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public int compareTo(ByteArrayWrapper val) {
        int i = 0;
        while (i<bytes.length) {
            if (bytes.length < val.bytes.length) {
                return -1;
            } else if (bytes.length > val.bytes.length) {
                return 1;
            } else if ((bytes[i] & 0xFF) < (val.bytes[i] & 0xFF)) {
                return -1;
            } else if ((bytes[i] & 0xFF) > (val.bytes[i] & 0xFF) ) {
                return  1;
            }
            i++;
        }
        return classifier == val.classifier ? 0 : (classifier > val.classifier ? 1 : -1);
    }

}