package net.imagini.aim.utils;

import java.nio.ByteBuffer;

public class Pointer {

    final public int offset;
    final public int size;
    final public byte[] array;
    final public ByteBuffer byteBuffer;
    public Pointer(ByteBuffer buffer) {
        this(buffer.array(), buffer.position(), buffer.limit() - buffer.position() );
    }
    public Pointer(byte[] array, int position, int size) {
        this.offset = position;
        this.array = array;
        this.size = size;
        this.byteBuffer = ByteBuffer.wrap(array, offset, size); 
    }
}
