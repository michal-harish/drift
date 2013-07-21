package net.imagini.aim;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

import org.apache.commons.io.EndianUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;

/**
 * TODO This class must be thread-safe as multiple loaders and queries can
 * be run simultaneously.
 * 
 * @author mharis
 *
 */
public class AimColumn {
    final public AimDataType type;
    final public Integer size;
    private LinkedList<ByteArrayOutputStream> segments;
    private LZ4BlockOutputStream lz4;
    private ByteArrayOutputStream segment;

    public AimColumn(AimDataType type) {
        this.type = type;
        if (type instanceof Aim.BYTEARRAY) {
            this.size = ((Aim.BYTEARRAY)type).size;
        } else {
            this.size = type.getSize();
        }
        this.segments = new LinkedList<ByteArrayOutputStream>();
    }
    private void openSegment() {
        if (lz4 == null) {
            segment = new ByteArrayOutputStream(65535);
            lz4 = new LZ4BlockOutputStream(
                segment, 
                65535,
                LZ4Factory.fastestInstance().fastCompressor(),
                XXHashFactory.fastestInstance().newStreamingHash32(0x9747b28c).asChecksum(), 
                true
            );
        }
    }
    public void flushSegment() throws IOException {
        if (lz4 != null) {
            lz4.flush();
        }
    }
    public void closeSegment() throws IOException {
        if (lz4 != null) {
            lz4.close();
            lz4 = null;
            segments.add(segment);
            segment = null;
        }
    }
    final public void write(int value) throws IOException {
        openSegment();
        EndianUtils.writeSwappedInteger(lz4,value);
    }
    public void write(byte[] value) throws IOException {
        openSegment();
        lz4.write(value);
    }
    public void write(String value) throws IOException {
        openSegment();
        if (value == null) {
            write((int) 0);
        } else {
            byte[] data = value.getBytes();
            write(data.length);
            write(data);
        }
    }

    public InputStream select() throws IOException {
        return new SegmentedInputStream(0, segments.size()-1);
    }
    public InputStream select(int segmentId) throws IOException {
        return new SegmentedInputStream(segmentId, segmentId);
    }

    private class SegmentedInputStream extends InputStream {
        private int segmentIndex = 0;
        private int endSegment = 0;
        private InputStream segmentStream;
        public SegmentedInputStream(int startSegment, int endSegment) throws IOException {
            this.segmentIndex = startSegment;
            this.endSegment = Math.min(endSegment, segments.size());
            nextSegment();
        }
        @Override public int read() throws IOException {
            int result = segmentStream.read();
            if (result == -1) {
                nextSegment();
                result = segmentStream.read();
            }
            return result;
        }
        private void nextSegment() throws EOFException {
            if (segmentIndex > endSegment) {
                throw new EOFException();
            }
            ByteArrayOutputStream segment = segments.get(segmentIndex++);
            segmentStream = new LZ4BlockInputStream(new ByteArrayInputStream(segment.toByteArray()));
        }
    }

    public int getLastSegmentId() {
        return segments.size()-1;
    }
}
