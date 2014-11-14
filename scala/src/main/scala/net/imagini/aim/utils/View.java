package net.imagini.aim.utils;

import java.io.IOException;
import java.nio.ByteBuffer;

public class View {

    public int offset =-1;
    public int size =-1;
    public byte[] array = null;

    public View() {}
    public View(View copy) {
        this.array = copy.array;
        this.size = copy.size;
        this.offset = copy.offset;
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

    public boolean eof() {
        return offset >= size;
    }

    public int skip() throws IOException {
        offset +=1;
        return 1;
    }
    @Override public String toString() {
        return offset + ":" + size + " " + this.getClass().getSimpleName();
    }
}
