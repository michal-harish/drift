package net.imagini.aim;

import java.nio.ByteBuffer;

import net.imagini.aim.utils.BlockStorageLZ4;

public class MemoryTest {

    static public void  main(String[] args) throws InterruptedException {
        new MemoryTest();
    }

    private BlockStorageLZ4[] buf = new BlockStorageLZ4[1000];

    public MemoryTest() throws InterruptedException {
        synchronized(this) {
            wait(10000);
        }
        for (int i=0; i< 100; i ++) {
            BlockStorageLZ4 l = new BlockStorageLZ4();
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
