package net.imagini.aim.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.imagini.aim.types.Aim;
import net.imagini.aim.types.AimType;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

public class Pipe {
    private Socket socket;
    final public static int LZ4_BLOCK_SIZE = 131070;
    final public static int SIGNATURE = "DRIFT".hashCode();
    final public Protocol protocol;
    final public int compression;
    private OutputStream outputPipe;
    private InputStream inputPipe = null;

    static public InputStream createInputPipe(
            InputStream underlyingInputStream, int compression)
            throws IOException {
        switch (compression) {
        case 0:
            return underlyingInputStream;
        case 1:
            return new LZ4BlockInputStream(underlyingInputStream);
        case 2:
        case 3:
            return new GZIPInputStream(underlyingInputStream);
        default:
            throw new IOException("Unsupported compression type " + compression);
        }
    }

    public static OutputStream createOutputPipe(
            OutputStream underlyingOutputStream, int compression)
            throws IOException {
        switch (compression) {
        case 0:
        case 3:
            return underlyingOutputStream;
        case 1:
            return new LZ4BlockOutputStream(underlyingOutputStream,
                    LZ4_BLOCK_SIZE, 
                    LZ4Factory.fastestInstance().fastCompressor(), 
                    XXHashFactory.fastestInstance().newStreamingHash32(0x9747b28c).asChecksum()
                    , true);
        case 2:
            return new GZIPOutputStream(underlyingOutputStream, true);
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

    final public void finishOutput() throws IOException {
        if (outputPipe != null) {
            if (outputPipe instanceof LZ4BlockOutputStream) {
                ((LZ4BlockOutputStream) outputPipe).finish();
            } else if (outputPipe instanceof GZIPOutputStream) {
                ((GZIPOutputStream) outputPipe).finish();
            } else {
              //TODO create end of output signature for uncompressed stream
            }
            outputPipe = null;
        }
    }

    final public void close() throws IOException {
        finishOutput();
        socket.close();
        inputPipe = null;
    }

    final public void writeHeader(String value) throws IOException {
        if (outputPipe != null)
            throw new IllegalStateException();
        StreamUtils.write(Aim.STRING, Aim.STRING.convert(value),
                socket.getOutputStream());
        socket.getOutputStream().flush();
    }

    public String readHeader() throws IOException {
        if (inputPipe != null)
            throw new IllegalStateException();
        String value = Aim.STRING.asString(StreamUtils.read(
                socket.getInputStream(), Aim.STRING));
        return value;
    }

    public InputStream getInputStream() throws IOException {
        if (inputPipe == null)
            inputPipe = createInputPipe(socket.getInputStream(), compression);
        return inputPipe;
    }

    final public void flush() throws IOException {
        if (outputPipe != null) {
            outputPipe.flush();
        }
    }

    public OutputStream getOutputStream() throws IOException {
        if (outputPipe == null)
            outputPipe = createOutputPipe(socket.getOutputStream(), compression);
        return outputPipe;
    }

    final public void writeInt(int value) throws IOException {
        if (outputPipe == null)
            outputPipe = createOutputPipe(socket.getOutputStream(), compression);
        StreamUtils.writeInt(outputPipe, value);
    }

    final public Pipe write(String value) throws IOException {
        if (outputPipe == null)
            outputPipe = createOutputPipe(socket.getOutputStream(), compression);
        StreamUtils.write(Aim.STRING, Aim.STRING.convert(value), outputPipe);
        return this;
    }

    public int write(AimType type, byte[] value) throws IOException {
        if (outputPipe == null)
            outputPipe = createOutputPipe(socket.getOutputStream(), compression);
        return StreamUtils.write(type, value, outputPipe);
    }

    public String read() throws IOException {
        if (inputPipe == null)
            inputPipe = createInputPipe(socket.getInputStream(), compression);
        return Aim.STRING.asString(read(Aim.STRING));
    }

    public int readInt() throws IOException {
        if (inputPipe == null)
            inputPipe = createInputPipe(socket.getInputStream(), compression);
        return StreamUtils.readInt(inputPipe);
    }

    public byte[] read(AimType type) throws IOException {
        if (inputPipe == null)
            inputPipe = createInputPipe(socket.getInputStream(), compression);
        return StreamUtils.read(inputPipe, type);
    }

    public void skip(AimType type) throws IOException {
        if (inputPipe == null)
            inputPipe = createInputPipe(socket.getInputStream(), compression);
        StreamUtils.skip(inputPipe, type);
    }

}
