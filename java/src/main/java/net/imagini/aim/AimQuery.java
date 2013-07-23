package net.imagini.aim;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

import net.imagini.aim.pipes.Pipe;


public class AimQuery {

    private AimTable table;
    private int startSegment;
    private int endSegment;

    public AimQuery(AimTable table) {
        this(table, null);
    }

    public AimQuery(AimTable table, Integer segmentRange) { 
        this.table = table;
        int segmentCount = table.getNumSegments();
        if (segmentRange == null) { //all segments
            startSegment = 0;
            endSegment = segmentCount-1;
        } else if (segmentRange < 0 ) { // latest -n segments
            startSegment = segmentCount + segmentRange;
            endSegment = segmentCount-1;
        } else {// single specific segment
            startSegment = endSegment = segmentRange;
        }
    }

    public AimFilter filter(String expression) {
        return AimFilter.proxy(table, startSegment, endSegment, expression);
    }

    @SuppressWarnings("serial")
    public Pipe select(final String... colNames) throws IOException {
        final InputStream[] range = table.range(startSegment, endSegment, colNames);
        final AimDataType[] types = new LinkedList<AimDataType>() {{
            for(String colName: colNames) add(table.def(colName));
        }}.toArray(new AimDataType[colNames.length]);

        return new Pipe() {
            private int fieldIndex = -1;
            @Override public byte[] read(AimDataType type) throws IOException {
                if (++fieldIndex == types.length) fieldIndex = 0;
                return Pipe.read(range[fieldIndex], types[fieldIndex]);
            }
        };
    }

    @SuppressWarnings("serial")
    public Pipe select(final BitSet filter, final String... colNames) throws IOException {
        final InputStream[] range = table.range(startSegment, endSegment, colNames);
        final AimDataType[] types = new LinkedList<AimDataType>() {{
            for(String colName: colNames) add(table.def(colName));
        }}.toArray(new AimDataType[colNames.length]);

        return new Pipe() {
            private int fieldIndex = types.length;
            private AtomicInteger index = new AtomicInteger(0);
            private AtomicInteger remaining = new AtomicInteger(filter.cardinality()); 
            @Override public byte[] read(AimDataType type) throws IOException {
                if (++fieldIndex >= types.length) {
                    fieldIndex = 0;
                    if (remaining.decrementAndGet() < 0)  throw new EOFException();
                    while (!filter.get(index.getAndIncrement())) {
                        for(int i=0; i<range.length; i++) {
                            Pipe.skip(range[i],types[i]);
                        }
                    }
                }
                return Pipe.read(range[fieldIndex], types[fieldIndex]);
            }
        };
    }

}
