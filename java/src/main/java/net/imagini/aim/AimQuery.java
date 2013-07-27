package net.imagini.aim;

import java.io.IOException;

import net.imagini.aim.AimTypeAbstract.AimDataType;
import net.imagini.aim.node.AimTable;
import net.imagini.aim.pipes.Pipe;

/**
 * @author mharis
 */
public class AimQuery {

    private AimTable table;
    private int startSegment;
    private int endSegment;


    public AimQuery(AimTable table) { 
        this.table = table;
    }

    public void range(Integer segmentRange) {
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

    public Pipe select(final String... colNames) throws IOException {
        final Pipe range = table.open(startSegment, endSegment, null, colNames);
        return new Pipe() {
            private int fieldIndex = -1;
            @Override public byte[] read(AimDataType type) throws IOException {
                if (++fieldIndex == colNames.length) fieldIndex = 0;
                return range.read(type);
            }
        };
    }

    public Pipe select(AimFilter filter, final String... colNames) throws IOException {
        final Pipe range = table.open(startSegment, endSegment, filter, colNames);
        return new Pipe() {
            private int fieldIndex = -1;
            @Override public byte[] read(AimDataType type) throws IOException {
                if (++fieldIndex == colNames.length) fieldIndex = 0;
                return range.read(type);
            }
        };
    }

/*
    public Pipe select(final AimFilterSet filter, final String... colNames) throws IOException {
        final Pipe range = table.open(startSegment, endSegment, colNames);
        return new Pipe() {
            private int fieldIndex = colNames.length;
            private AtomicLong index = new AtomicLong(0);
            private AtomicLong remaining = new AtomicLong(filter.cardinality()); 
            @Override public byte[] read(AimDataType type) throws IOException {
                if (++fieldIndex >= colNames.length) {
                    fieldIndex = 0;
                    if (remaining.decrementAndGet() < 0)  throw new EOFException();
                    while (!filter.get(index.getAndIncrement())) {
                        for(int i=0; i<colNames.length; i++) {
                            range.skip(type);
                        }
                    }
                }
                return range.read(type);
            }
        };
    }
*/

}
