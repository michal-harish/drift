package net.imagini.aim.utils;

import java.util.Arrays;

public class ByteKey implements Comparable<ByteKey> {
    final public byte[] bytes;
    final public int limit;
    final public int size;
    final public int offset;
    final public int classifier;

    public ByteKey(byte[] data) {
        this(data, 0, data.length, 0);
    }
    public ByteKey(View data, int classifier) {
        this(data.array, data.offset, data.limit - data.offset + 1, classifier);
    }
    public ByteKey(byte[] data, int offset, int size, int classifier) {
        if (data == null) {
            throw new NullPointerException();
        }
        this.classifier = classifier;
        this.bytes = data;
        this.size = size;
        this.limit = size + offset;
        this.offset = offset;
    }

    @Override public boolean equals(Object other) {
        if (other == null || !(other instanceof ByteKey)) {
            return false;
        }
        ByteKey otherKey = ((ByteKey)other);
        return otherKey.classifier == classifier  && compareTo(otherKey) == 0;
    }

    @Override public int hashCode() {
        return Arrays.hashCode(bytes) + classifier;
    }

    @Override
    public int compareTo(ByteKey val) {
        int i = offset;
        int j = val.offset;
        while (i<limit) {
            if (size < val.size) {
                return -1;
            } else if (size > val.size) {
                return 1;
            } else if ((bytes[i] & 0xFF) < (val.bytes[j] & 0xFF)) {
                return -1;
            } else if ((bytes[i] & 0xFF) > (val.bytes[j] & 0xFF) ) {
                return  1;
            }
            i++;
            j++;
        }
        return classifier == val.classifier ? 0 : (classifier > val.classifier ? 1 : -1);
    }

}
