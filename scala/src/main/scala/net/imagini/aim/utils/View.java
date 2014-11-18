package net.imagini.aim.utils;

import java.io.IOException;
import java.nio.ByteBuffer;

public class View implements Comparable<View> {

    public int offset = -1;

    public int size;
    public byte[] array = null;
    public int limit;

    public View(View copy) {
        clone(copy);
    }
    public void clone(View copy) {
        this.array = copy.array;
        this.size = copy.size;
        this.offset = copy.offset;
        this.limit = copy.limit;
    }

    public View(ByteBuffer buffer) {
        this(buffer.array(), buffer.position(), buffer.limit(), buffer.capacity() - buffer.position());
    }

    public View(byte[] array) {
        this(array, 0, array.length, array.length);
    }

    public View(byte[] array, int position, int limit, int size) {
        this.offset = position;
        this.array = array;
        this.size = size;
        this.limit = limit;
    }

    public boolean available(int numBytes) {
        if (numBytes == -1) {
            if (!available(4)) {
                return false;
            } else {
                numBytes = ByteUtils.asIntValue(array, offset) + 4;
            }
        }
        if (offset + numBytes > size) {
            return false;
        } else {
            limit = offset + numBytes - 1;
            return true;
        }
    }

    public void skip() throws IOException {
        offset = limit + 1;
    }

    @Override
    public String toString() {
        return offset + ":" + size + " " + this.getClass().getSimpleName();
    }

    @Override
    public boolean equals(Object other) {
        return (other == null || !(other instanceof View)) ? false
                : compareTo((View) other) == 0;
    }

    @Override
    public int hashCode() {
        return ByteUtils.asIntValue(array, offset);
    }

    @Override
    public int compareTo(View val) {
        int i = offset;
        int j = val.offset;
        int l = offset + Math.max(limit - offset, val.limit - val.offset);
        while (i < l) {
            if (i > limit) {
                return -1;
            } else if (j > val.limit) {
                return 1;
            } else if ((array[i] & 0xFF) < (val.array[j] & 0xFF)) {
                return -1;
            } else if ((array[i] & 0xFF) > (val.array[j] & 0xFF)) {
                return 1;
            }
            i++;
            j++;
        }
        return 0;
    }
}
