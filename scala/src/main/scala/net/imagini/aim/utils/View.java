package net.imagini.aim.utils;

import java.nio.ByteBuffer;

public class View {

    public int offset =-1;
    public int size =-1;
    public byte[] array = null;

    public View() {}
    public View(View copy) {
        set(copy);
    }
    public View(ByteBuffer buffer) {
        this(buffer.array(), buffer.position(), buffer.capacity() - buffer.position() );
    }

    public View(byte[] array) {
        this(array, 0, array.length);
    }

    public View(byte[] array, int position, int size) {
        this.offset = position;
        this.array = array;
        this.size = size;
    }
    public void set(View view) {
        this.array = view.array;
        this.size = view.size;
        this.offset = view.offset;
    }

    public boolean eof() {
        return offset >= size;
    }
    public void rewind() {
        offset = 0;
    }
    public int skip() {
        offset +=1;
        return 1;
    }
    @Override public String toString() {
        return offset + ":" + size;
    }
}
