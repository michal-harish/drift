package net.imagini.aim;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

public class PipeLZ4 extends Pipe {
     final public static Integer LZ4_BLOCK_SIZE = 524280;
     private LZ4BlockOutputStream lz4OutputStream;

     public PipeLZ4(OutputStream out) throws IOException {
        super(out);
     }
     public PipeLZ4(OutputStream out,  Protocol protocol) throws IOException {
         super(out, protocol);
     }
     public PipeLZ4(InputStream in) throws IOException {
         super(in);
     }

     public PipeLZ4(InputStream in, Protocol protocol) throws IOException {
        super(in, protocol);
    }
    public PipeLZ4(Socket socket, Protocol protocol) throws IOException {
        super(socket, protocol);
    }
    @Override protected OutputStream createOutputStreamWrapper(OutputStream out) throws IOException {
         lz4OutputStream = new LZ4BlockOutputStream(
             out, 
             LZ4_BLOCK_SIZE, 
             LZ4Factory.fastestInstance().highCompressor(),
             XXHashFactory.fastestInstance().newStreamingHash32(0x9747b28c).asChecksum(), 
             true
         );
         return lz4OutputStream;
     }

     @Override protected InputStream createInputStreamWrapper(InputStream in) throws IOException {
         return new LZ4BlockInputStream(in);
     }

}
