package net.imagini.aim;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedList;

import net.imagini.aim.AimTypeAbstract.AimDataType;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Decompressor;
import net.jpountz.lz4.LZ4Factory;

/**
 * This is an lz4 buffer for ZeroCopy filtering and scanning.
 * 
 * Although we've used here direct buffer, it seems that it doesn't
 * give that much performance - the zero copy approach itself has much larger effect -
 * so given the headaches with memory leaks we could potentially switch to array-backed ByteBuffer
 * 
 * TODO investigate possibility of LZ4Buffer<AimDataType>
 * 
 * @author mharis
 */
public class LZ4Buffer {

    private Integer size = 0;
    LinkedList<byte[]> blocks = new LinkedList<byte[]>();
    LinkedList<Integer> lengths = new LinkedList<Integer>();
    Integer block = null;
    ByteBuffer zoom;

    static private LZ4Compressor compressor = LZ4Factory.fastestInstance().highCompressor();
    static private LZ4Decompressor decompressor = LZ4Factory.fastestInstance().decompressor();

    static public void main(String[] args) {

        LZ4Buffer instance = new LZ4Buffer();
        instance.addBlock((
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "\n"
        ).getBytes());

        instance.addBlock((
            "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
            "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
            "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
            "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
            "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
            "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
            "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
            "\n"
        ).getBytes());

        instance.rewind();

        byte[] b = new byte[1];
        while (!instance.eof()) {
            b[0] = instance.read();
            System.out.print(new String(b));
        }
    }

    public LZ4Buffer() {
        block = null;
        zoom = null;
    }

    public void addBlock(byte[] block) {
        int blockLength = block.length;
        byte[] in = new byte[compressor.maxCompressedLength(blockLength)]; 
        int cLen = compressor.compress(block, 0, blockLength, in, 0);
        ByteBuffer out = ByteBuffer.allocate(4 + cLen);
        out.order(ByteOrder.LITTLE_ENDIAN);
        out.putInt(blockLength);
        out.put(in,0,cLen);
        out.flip();
        blocks.add(out.array());
        lengths.add(blockLength);
        size += cLen;
    }

    public long size() {
        return size;
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

    public void mark() {
        zoom.mark();
    }

    public void reset() {
        zoom.reset();
    }

    public int skip(int skipBytes) {
        int skipped = Math.min(skipBytes,zoom.remaining());
        zoom.position(zoom.position()+skipBytes);
        return skipped;
    }


    public int compare(ByteBuffer value, AimDataType type){

        int ni; int i = zoom.position();
        int nj; int j = 0;
        if (type.equals(Aim.STRING)) {
            zoom.mark();
            value.mark();
            ni = zoom.getInt() + 4; ni += 4;
            nj = value.getInt() + 4; nj += 4;
            zoom.reset();
            value.reset();
        } else {
            ni = nj = type.getSize();
        }
        if (ni == nj) {
            int n = Math.min(ni, nj);
            for (; j < n; i++, j++) {
                int cmp = Byte.compare(zoom.get(i), value.get(j));
                if (cmp != 0)
                    return cmp;
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
            zoom.mark();
            value.mark();
            ni = zoom.getInt() + 4; i += 4;
            nj = value.getInt() + 4; j += 4;
            zoom.reset();
            value.reset();
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

    public int asIntValue() {
        return zoom.asIntBuffer().get(0);
    }

    public byte read() {
        return zoom.get();
    }

    private boolean decompress(Integer block) {
        if (block != null && this.block == block) {
            return true;
        }
        if (block >= blocks.size()) {
            return false;
        }
        this.block = block;
        int length = lengths.get(block);
        byte[] r = new byte[length];
        decompressor.decompress(blocks.get(block), 4, r, 0, length);

        /**
         * malloc() buffer ! instaed of zoom = ByteBuffer.wrap(r);
         */
        zoom = ByteBuffer.allocateDirect(r.length);
        zoom.put(r);
        zoom.flip();

        zoom.order(Aim.endian);
        zoom.rewind();
        return true;
    }

}
