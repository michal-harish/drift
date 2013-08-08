package net.imagini.aim;

import java.nio.ByteBuffer;
import java.util.LinkedList;

import scala.actors.threadpool.Arrays;

import net.imagini.aim.AimTypeAbstract.AimDataType;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Decompressor;
import net.jpountz.lz4.LZ4Factory;

/**
 * This is an lz4 buffer for ZeroCopy filtering and scanning.
 * 
 * It is not Thread-Safe as it is a shared storage.
 * For Thread-Safe operations over an instance of LZ4Buffer use LZ4Scanner.
 * 
 * Although we've tried to use direct buffers, it seems that it doesn't
 * give that much performance - the zero copy approach itself has much larger effect -
 * so given the headaches with memory leaks we user array-backed ByteBuffer.
 * 
 * TODO decompress_buffer needs to be verified for Thread-Safety in the LZ4 c library
 * TODO investigate possibility of LZ4Buffer<AimDataType>
 * 
 * @author mharis
 */
public class LZ4Buffer {

    protected Integer size = 0;
    protected LinkedList<byte[]> compressedBlocks = new LinkedList<byte[]>();
    protected LinkedList<Integer> lengths = new LinkedList<Integer>();

    static private Object compress_lock = new Object();
    static private byte[] compress_buffer = new byte[65535];
    static private LZ4Compressor compressor = LZ4Factory.fastestInstance().highCompressor();
    static protected LZ4Decompressor decompressor = LZ4Factory.fastestInstance().decompressor();

    static public void main(String[] args) {

        //TODO move lz4buffer static main to a test class
        LZ4Buffer instance = new LZ4Buffer();
        instance.addBlock(ByteBuffer.wrap((
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "\n"
        ).getBytes()));

        instance.addBlock(ByteBuffer.wrap((
            "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
            "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
            "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
            "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
            "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
            "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
            "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
            "\n"
        ).getBytes()));

        LZ4Scanner scanner = new LZ4Scanner(instance);
        byte[] b = new byte[1];
        while (!scanner.eof()) {
            b[0] = scanner.read();
            System.out.print(new String(b));
        }
    }


    public int addBlock(ByteBuffer block) {

        int blockLength = block.limit();
        int maxCLen = compressor.maxCompressedLength(blockLength);
        synchronized(compress_lock) {
            if (compress_buffer.length < maxCLen) {
                compress_buffer = new byte[maxCLen+65535];
            }
            int cLen = compressor.compress(block.array(), 0, blockLength, compress_buffer, 0);
            compressedBlocks.add(Arrays.copyOfRange(compress_buffer, 0, cLen));
            lengths.add(blockLength);
            size += cLen;
            return cLen;
        }
    }

    public long size() {
        return size;
    }

    static public class LZ4Scanner {
        private LZ4Buffer buffer;
        private byte[] decompress_buffer;
        private Integer block = null;
        private ByteBuffer zoom;
        private int maxBlock;
        public LZ4Scanner(LZ4Buffer buffer) {
            this.buffer = buffer;
            this.maxBlock = buffer.compressedBlocks.size() -1;
            rewind();
        }
        public void rewind() {
            block = null;
            zoom = null;
        }
        public boolean eof() {
            if (zoom == null) {
                if (!decompress(0)) {
                    return true;
                }
            }
            if (zoom.position() == zoom.limit()) {
                if (!decompress(block+1)) {
                    return true;
                }
            }
            return false;
        }
        public long skip(long skipBytes) {
            if (skipBytes > zoom.remaining()) {
                skipBytes = zoom.remaining();
            }
            if (skipBytes > 0) {
                zoom.position(zoom.position()+(int)skipBytes);
            }
            return skipBytes;
        }
        public int asIntValue() {
            return AimUtils.getIntegerValue(zoom);
        }

        public byte read() {
            return zoom.get();
        }

        public int compare(ByteBuffer value, AimDataType type){
            int ni; int i = zoom.position();
            int nj; int j = 0;
            int n;
            if (type.equals(Aim.STRING)) {
                ni = AimUtils.getIntegerValue(zoom) + 4; i += 4;
                nj = AimUtils.getIntegerValue(value) + 4; j += 4;
                n = Math.min(ni, nj);
            } else {
                n = ni = nj = type.getSize();
            }
            if (ni == nj) {
                for (; j < n; i++, j++) {
                    int cmp = Byte.compare(zoom.get(i), value.get(j));
                    if (cmp != 0) {
                        return cmp;
                    }
                }
            }
            return ni - nj;
        }

        /**
         * TODO this routine should be done a bit better 
         */
        public boolean contains(ByteBuffer value, AimDataType type) {
            int ni; int i = zoom.position();
            int nj; int j = 0;
            if (type.equals(Aim.STRING)) {
                ni = AimUtils.getIntegerValue(zoom) + 4; i += 4;
                nj = AimUtils.getIntegerValue(value) + 4; j += 4;
            } else {
                ni = nj = type.getSize();
            }
            if (nj > ni) {
                return false; 
            } else {
                ni += zoom.position();
                int v = j; for (; i < ni; i++) {
                    byte b = zoom.get(i);
                    if (value.get(v)!=b) {
                        v = j;
                    } else if (++v==value.limit()) {
                        return true;
                    }
                }
                return false;
            }
        }
        private boolean decompress(Integer block) {
            if (block == null || this.block != block) {
                if (block > maxBlock) {
                    return false;
                }
                this.block = block;
                int length = buffer.lengths.get(block);
                if (decompress_buffer == null || decompress_buffer.length<length) {
                    decompress_buffer = new byte[length];
                }
                LZ4Buffer.decompressor.decompress(
                    buffer.compressedBlocks.get(block), 
                    0, decompress_buffer, 0, length
                );

                zoom = ByteBuffer.wrap(decompress_buffer,0,length);
                /**
                 * Direct malloc() 
                 *
                zoom = ByteBuffer.allocateDirect(length);
                zoom.put(decompress_buffer, 0, length);
                zoom.flip();*/

                zoom.order(Aim.endian);
            }
            zoom.rewind();
            return true;
        }
    }
}
