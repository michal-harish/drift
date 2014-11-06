package net.imagini.aim.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.imagini.aim.tools.StreamUtils;
import net.imagini.aim.types.Aim;
import net.imagini.aim.types.AimDataType;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

/**
 * Pipe is a bi-directional stream that is aimtype-aware
 * 
 * @author mharis
 */
public class Pipe {
    private Socket socket;
    final public static int LZ4_BLOCK_SIZE = 524280;
    final public static int SIGNATURE = "DRIFT".hashCode();
    final public Protocol protocol;
    final public int compression;
    private OutputStream outputPipe;
    private InputStream inputPipe = null;

    private void createInputPipe() throws IOException {
        switch (compression) {
        case 0:
            inputPipe = socket.getInputStream();
            break;
        case 1:
            inputPipe = new LZ4BlockInputStream(socket.getInputStream());
            break;
        case 2: case 3:
//            System.err.println("GZIP INPUT INIT ");
            inputPipe = new GZIPInputStream(socket.getInputStream());
            break;
        default:
            throw new IOException("Unsupported compression type " + compression);
        }
    }

    private void createOutputPipe() throws IOException {
        switch (compression) {
        case 0: case 3:
            outputPipe = socket.getOutputStream();
            break;
        case 1:
            outputPipe = new LZ4BlockOutputStream(socket.getOutputStream(),
                    LZ4_BLOCK_SIZE, LZ4Factory.fastestInstance()
                            .highCompressor(), XXHashFactory.fastestInstance()
                            .newStreamingHash32(0x9747b28c).asChecksum(), true);
            break;
        case 2:
//            System.err.println("GZIP OUTPUT INIT ");
            outputPipe = new GZIPOutputStream(socket.getOutputStream(), true);
            break;
        default:
            throw new IOException("Unsupported compression type " + compression);

        }
    }

    public static Pipe newLZ4Pipe(Socket socket, Protocol protocol)
            throws IOException {
        return new Pipe(socket, protocol, 1);
    }

    public static Pipe newGZIPPipe(Socket socket, Protocol protocol)
            throws IOException {
        return new Pipe(socket, protocol, 2);
    }

    public Pipe(Socket socket, Protocol protocol, int compression)
            throws IOException {
        this.socket = socket;
        this.protocol = protocol;
        StreamUtils.writeInt(socket.getOutputStream(), SIGNATURE);
        StreamUtils.writeInt(socket.getOutputStream(), compression);
        StreamUtils.writeInt(socket.getOutputStream(), protocol.id);
        this.compression = compression == 3 ? 0 : compression;
        socket.getOutputStream().flush();
    }

    public Pipe(Socket socket) throws IOException {
        this.socket = socket;
        int signature = StreamUtils.readInt(socket.getInputStream());
        if (signature != SIGNATURE) {
            throw new IOException("Invalid pipe header signature");
        }
        this.compression = StreamUtils.readInt(socket.getInputStream());
        int pipe_protocol = StreamUtils.readInt(socket.getInputStream());
        this.protocol = Protocol.get(pipe_protocol);
    }

    final public void close() throws IOException {
        if (inputPipe != null) inputPipe.close();
        inputPipe = null;
        if (outputPipe != null) {
            if (outputPipe instanceof LZ4BlockOutputStream) {
                ((LZ4BlockOutputStream)outputPipe).finish();
            }
            outputPipe.close();
        }
        outputPipe = null;
    }
    final public void writeHeader(String value) throws IOException {
        if (outputPipe != null) throw new IllegalStateException();
        StreamUtils.write(Aim.STRING, Aim.STRING.convert(value), socket.getOutputStream());
//        System.err.println("HEADER > " + value);
        socket.getOutputStream().flush();
    }

    public String readHeader() throws IOException {
        if (inputPipe != null) throw new IllegalStateException();
        String value = Aim.STRING.convert(StreamUtils.read(socket.getInputStream(), (Aim.STRING)));
//        System.err.println("HEADER << " + value);
        return value;
    }

    public InputStream getInputStream() throws IOException {
        if (inputPipe == null) createInputPipe();
        return inputPipe;
    }

    final public void flush() throws IOException {
        if (outputPipe != null) {
            outputPipe.flush();
        }
    }

    public OutputStream getOutputStream() throws IOException {
        if (outputPipe == null) createOutputPipe();
        return outputPipe;
    }

    final public void writeInt(int value) throws IOException {
        if (outputPipe == null) createOutputPipe();
        StreamUtils.writeInt(outputPipe, value);
    }

    final public Pipe write(String value) throws IOException {
        if (outputPipe == null) createOutputPipe();
        StreamUtils.write(Aim.STRING, Aim.STRING.convert(value), outputPipe);
        return this;
    }

    public int write(AimDataType type, ByteBuffer value) throws IOException {
        if (outputPipe == null) createOutputPipe();
        return StreamUtils.write(type, value, outputPipe);
    }

    public int write(AimDataType type, byte[] value) throws IOException {
        if (outputPipe == null) createOutputPipe();
        return StreamUtils.write(type, value, outputPipe);
    }

    public int readInto(AimDataType type, ByteBuffer buf) throws IOException {
        if (inputPipe == null) createInputPipe();
        return StreamUtils.read(inputPipe, type, buf);
    }

    public String read() throws IOException {
        if (inputPipe == null) createInputPipe();
        return Aim.STRING.convert(read(Aim.STRING));
    }

    public int readInt() throws IOException {
        if (inputPipe == null) createInputPipe();
        return StreamUtils.readInt(inputPipe);
    }

    public byte[] read(AimDataType type) throws IOException {
        if (inputPipe == null) createInputPipe();
        return StreamUtils.read(inputPipe, type);
    }

    public void skip(AimDataType type) throws IOException {
        if (inputPipe == null) createInputPipe();
        StreamUtils.skip(inputPipe, type);
    }

}
