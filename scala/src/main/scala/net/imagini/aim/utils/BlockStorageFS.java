package net.imagini.aim.utils;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import net.imagini.aim.cluster.StreamUtils;
import net.imagini.aim.utils.BlockStorage.PersistentBlockStorage;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Decompressor;
import net.jpountz.lz4.LZ4Factory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockStorageFS extends BlockStorage implements
        PersistentBlockStorage {

    private static final Logger log = LoggerFactory
            .getLogger(BlockStorageFS.class);

    private static final String BASE_PATH = "/var/lib/drift/";

    final private File file;
    private LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();;
    private LZ4Decompressor decompressor = LZ4Factory.fastestInstance()
            .decompressor();
    private OutputStream fout;
    final private byte[] compressBuffer;

    public BlockStorageFS(String args) throws IOException {
        super();
        if (args == null || args.isEmpty()) {
            throw new IllegalArgumentException(
                    "BlockStorageFS requires argument for relative path");
        }
        String localId = args;
        new File(BASE_PATH).mkdirs();
        this.file = new File(BASE_PATH + localId + ".lz");
        this.file.getParentFile().mkdirs();
        this.compressBuffer = new byte[compressor.maxCompressedLength(blockSize())];
        if (file.exists()) {
            storedSize.set(file.length());
        } else {
            fout = new FileOutputStream(file);
        }
    }

    @Override
    public int blockSize() {
        return 8192; // 8Kb
    }

    @Override
    protected int storeBlock(byte[] array, int offset, int length)
            throws IOException {
        int compressedLen = compressor.compress(array, offset, length,
                compressBuffer, 0);
        StreamUtils.writeInt(fout, length);
        StreamUtils.writeInt(fout, compressedLen);
        fout.write(compressBuffer, 0, compressedLen);
        fout.flush();
        return compressedLen;
    }

    @Override
    public View toView() throws Exception {
        final InputStream fin = new FileInputStream(file);
        return new View(new byte[blockSize()], 0, -1, 0) {
            boolean eof = false;
            @Override public boolean available(int numBytes) {
                if (!super.available(numBytes)) {
                    if (!readNextBlock()) {
                        eof = true;
                        try {
                            fin.close(); 
                        } catch (IOException e) {}
                        return false;
                    } else {
                        return super.available(numBytes);
                    }
                } else {
                    return true;
                }
            }
            private boolean readNextBlock() {
                if (eof) return false; else try {
                    limit = 0;
                    offset = 0;
                    size = StreamUtils.readInt(fin);
                    int cLen = StreamUtils.readInt(fin);
                    StreamUtils.read(fin, compressBuffer, 0, cLen);
                    decompressor.decompress(compressBuffer, 0, array, 0, size);
                    return true;
                } catch (EOFException e) {
                    return false;
                } catch (IOException e) {
                    log.error("Could not open lz4 block from " + file.getAbsolutePath(), e);
                    return false;
                }
            }
        };
    }

}
