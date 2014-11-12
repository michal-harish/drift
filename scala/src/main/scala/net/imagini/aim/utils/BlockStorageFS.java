package net.imagini.aim.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import net.imagini.aim.cluster.Pipe;
import net.imagini.aim.tools.StreamUtils;

public class BlockStorageFS extends BlockStorage {

    private static final String BASE_PATH = "/var/lib/drift/";
    private final static AtomicLong refIdHack = new AtomicLong(0);

    private final String path;
    private AtomicInteger numBlocks = new AtomicInteger(0);
    private final AtomicLong size = new AtomicLong(0);

    final private int compression; //0=None, 1-LZ4, 2-GZIP

    public BlockStorageFS(/* String identifier, int compression */) {
        String identifier = "drift-test-" + refIdHack.incrementAndGet();
        this.compression = 1;
        this.path = BASE_PATH  + identifier + "/";
        File pathHandle = new File(path);
        pathHandle.mkdir();
    }

    @Override
    protected int blockSize() {
        return 1048576 * 4; // 4Mb
    }

    @Override
    protected int storeBlock(byte[] array, int offset, int length)
            throws IOException {
        int b = numBlocks.getAndIncrement();
        File blockFile = new File(path + b);
        OutputStream fout = Pipe.createOutputPipe(new FileOutputStream(blockFile), compression);
        fout.write(array, offset, length);
        fout.flush();
        fout.close();
        size.addAndGet(length);
        return length;
    }

    @Override
    public int numBlocks() {
        return numBlocks.get();
    }

    @Override
    public long storedSize() {
        return size.get();
    }

    @Override
    public long originalSize() {
        return size.get();
    }

    @Override
    protected byte[] load(int block) throws IOException {
        File blockFile = new File(path + block);
        InputStream fin = Pipe.createInputPipe(new FileInputStream(blockFile), compression);
        byte[] result = new byte[lengths.get(block)];
        StreamUtils.read(fin, result, 0, result.length);
        fin.close();
        return result;
    }

}
