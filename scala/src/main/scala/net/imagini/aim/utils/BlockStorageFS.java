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
import net.imagini.aim.utils.BlockStorage.PersistentBlockStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockStorageFS extends BlockStorage implements
        PersistentBlockStorage {

    private static final Logger log = LoggerFactory
            .getLogger(BlockStorageFS.class);

    private static final String BASE_PATH = "/var/lib/drift/";

    private final String path;
    private AtomicInteger numBlocks = new AtomicInteger(0);
    private final AtomicLong originalSize = new AtomicLong(0);
    private final AtomicLong storedSize = new AtomicLong(0);
    private final byte[] hackHotSpotBuffer = createHotSpotBuffer();

    final private byte[] createHotSpotBuffer() {
        return new byte[blockSize()];// this is only temporary until
                                     // memOptimisation can be turned on
    }

    final private int compression; // 0->None, 1->LZ4, 2->GZIP

    public BlockStorageFS(String args) throws IOException {
        if (args == null || args.isEmpty()) {
            throw new IllegalArgumentException(
                    "BlockStorageFS requires argument for relative path");
        }
        String localId = args;
        this.compression = 1;
        this.path = BASE_PATH + localId + "/";
        File p = new File(path);
        p.mkdirs();
        for (File blockFile : p.listFiles()) {
            int b = Integer.valueOf(blockFile.getName().split("\\.")[0]);
            numBlocks.set(Math.max(numBlocks.get(), b + 1));
            log.debug("Opening block " + blockFile.getAbsolutePath());
            storedSize.addAndGet(blockFile.length());
            InputStream bfin = Pipe.createInputPipe(new FileInputStream(
                    blockFile), compression);
            originalSize.addAndGet(StreamUtils.readInt(bfin));
            bfin.close();
        }
    }

    private File blockFile(int block) {
        switch (compression) {
        case 0:
            return new File(path + block);
        case 1:
            return new File(path + block + ".lz");
        case 2:
            return new File(path + block + ".gz");
        default:
            throw new IllegalArgumentException();
        }
    }

    @Override
    protected int blockSize() {
        return 1048576; // 1Mb
    }

    @Override
    protected int storeBlock(byte[] array, int offset, int length)
            throws IOException {
        File blockFile = blockFile(numBlocks.getAndIncrement());
        OutputStream fout = Pipe.createOutputPipe(new FileOutputStream(
                blockFile), compression);
        StreamUtils.writeInt(fout, length);
        fout.write(array, offset, length);
        fout.flush();
        fout.close();
        originalSize.addAndGet(length);
        storedSize.addAndGet(blockFile.length()); // FIXME this actually adds
                                                  // the uncompressed size for
                                                  // some reason
        return length;
    }

    @Override
    public int numBlocks() {
        return numBlocks.get();
    }

    @Override
    public long storedSize() {
        return storedSize.get();
    }

    @Override
    public long originalSize() {
        return originalSize.get();
    }

    @Override
    protected byte[] load(int block) throws IOException {
        File blockFile = blockFile(block);
        InputStream fin = Pipe.createInputPipe(new FileInputStream(blockFile),
                compression);
        int len = StreamUtils.readInt(fin);
        lengths.add(block, len);
        StreamUtils.read(fin, hackHotSpotBuffer, 0, len);
        fin.close();
        return hackHotSpotBuffer;
    }

}
