package net.imagini.aim.utils;

import java.util.Arrays;

public class ByteKey implements Comparable<ByteKey> {
    final public byte[] bytes;
    final public int classifier;

    public ByteKey(byte[] data) {
        this(data,0);
    }
    public ByteKey(byte[] data, int classifier) {
        if (data == null) {
            throw new NullPointerException();
        }
        this.classifier = classifier;
        this.bytes = data;
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof ByteKey)) {
            return false;
        }
        ByteKey otherKey = ((ByteKey)other);
        return otherKey.classifier == classifier  && Arrays.equals(bytes, otherKey.bytes);
    }

    @Override public int hashCode() {
        return Arrays.hashCode(bytes) + classifier;
    }

    @Override
    public int compareTo(ByteKey val) {
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
