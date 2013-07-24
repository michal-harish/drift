package net.imagini.aim;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicLong;

import net.imagini.aim.pipes.Pipe;
import net.imagini.aim.pipes.PipeLZ4;

public class AimSegmentConnection implements AimSegment {
    private AimSchema schema;
    private Socket socket;
    private Pipe remotePipe;
    private AtomicLong originalSize = new AtomicLong(0);

    public AimSegmentConnection(AimSchema schema, int port) throws IOException {
        this.schema = schema;
        socket = new Socket(
            InetAddress.getByName("localhost"),
            port
        );
        //TODO generic hand-shake (pipe_type,schema)
        this.remotePipe = new PipeLZ4(socket.getInputStream(), socket.getOutputStream());
    }

    @Override
    public void append(Pipe pipe) throws IOException {
        try {
            //TODO checkWritable(true);
            try {
                for(int col = 0; col < schema.size() ; col++) {
                    AimDataType type = schema.def(col); 
                    byte[] value = pipe.read(type);
                    originalSize.getAndAdd(remotePipe.write(type,value));
                }
            } catch (EOFException e) {
                close();
                throw e;
            }
        } catch (IllegalAccessException e1) {
            throw new IOException(e1);
        }
    }

    @Override
    public void close() throws IllegalAccessException, IOException {
        remotePipe.close();
        socket.close();
    }

    @Override
    public long getSize() {
        return 0;
    }

    @Override
    public long getOriginalSize() {
        return originalSize.get();
    }

    @Override
    public InputStream open(int column) throws IOException {
        // TODO send instruction over the socket
        return null;
    }

    @Override
    public Integer filter(AimFilter filter, BitSet segmentResult)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

}
