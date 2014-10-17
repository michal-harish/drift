package net.imagini.aim;

import java.nio.ByteBuffer;

import net.imagini.aim.LZ4Buffer;

public class MemoryTest {

    static public void  main(String[] args) throws InterruptedException {
        new MemoryTest();
    }

    private LZ4Buffer[] buf = new LZ4Buffer[1000];

    public MemoryTest() throws InterruptedException {
        synchronized(this) {
            wait(10000);
        }
        for (int i=0; i< 100; i ++) {
            LZ4Buffer l = new LZ4Buffer();
            l.addBlock(ByteBuffer.allocate(1000000));
            buf[i] = l;
        }
        synchronized(this) {
            wait(10000);
        }
        for (int i=0; i< 1000; i ++) {
            buf[i] = null;
        }
        synchronized(this) {
            wait(600000);
        }
    }
}
