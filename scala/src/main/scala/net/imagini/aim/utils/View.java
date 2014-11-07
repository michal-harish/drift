package net.imagini.aim.utils;

import java.nio.ByteBuffer;

public class View {

    public int offset;
    final public int size;
    final public byte[] array;

    public View(ByteBuffer buffer) {
        this(buffer.array(), buffer.position(), buffer.capacity() - buffer.position() );
    }
    public View(View copyOf) {
        this(copyOf.array, copyOf.offset, copyOf.size );
    }

    public View(byte[] array) {
        this(array, 0, array.length);
    }

    public View(byte[] array, int position, int size) {
        this.offset = position;
        this.array = array;
        this.size = size;
    }

    public void rewind() {
        offset = 0;
    }
    @Override public String toString() {
        return offset + ":" + size;
    }
}
