package net.imagini.aim;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;

public class PipeLZ4 extends Pipe {
     private LZ4BlockOutputStream lz4OutputStream;
     public PipeLZ4(OutputStream out) throws IOException {
        super(out);
     }

     public PipeLZ4(InputStream in) throws IOException {
         super(in);
     }

     @Override protected OutputStream getOutputPipe(OutputStream out) throws IOException {
         lz4OutputStream = new LZ4BlockOutputStream(out);//, 33554432);
         return lz4OutputStream;
     }

     @Override protected InputStream getInputPipe(InputStream in) throws IOException {
         return new LZ4BlockInputStream(in);
     }

}
