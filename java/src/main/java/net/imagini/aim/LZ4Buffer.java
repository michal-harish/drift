package net.imagini.aim;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedList;

import net.imagini.aim.AimTypeAbstract.AimDataType;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Decompressor;
import net.jpountz.lz4.LZ4Factory;

//TODO investigate possibility of LZ4Buffer<AimDataType>
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
        if (!eof()) {
            zoom.mark();
        }
    }

    public void reset() {
        if (!eof()) {
            zoom.reset();
        }
    }
/*
    public ByteBuffer getValue(AimType type) {
        zoom.slice().
    }

    public int compareTo(AimDataType type, ByteBuffer value) throws EOFException {
        if (eof()) {
            throw new EOFException();
        }

        return zoom.compareTo(value);
    }
    */
    
    public int read(AimDataType dataType, byte[] b) throws EOFException {
        if (!eof()) {
            int typeSize = 0;
            if (dataType.equals(Aim.STRING)) {
                zoom.mark();
                typeSize = zoom.getInt() + 4;
                zoom.reset();
            } else {
                typeSize = dataType.getSize();
            }
            zoom.get(b, 0, typeSize);
            return typeSize;
        } else {
            throw new EOFException();
        }
    }

    public byte read() {
        if (!eof()){
            return zoom.get();
        } else {
            return -1;
        }
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
        zoom = ByteBuffer.wrap(r);
        zoom.order(Aim.endian);
        zoom.rewind();
        return true;
    }

}
