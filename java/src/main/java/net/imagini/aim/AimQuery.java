package net.imagini.aim;

import java.io.IOException;

import net.imagini.aim.types.AimDataType;

/**
 * @author mharis
 */
public class AimQuery {

    private AimPartition table;
    private int startSegment;
    private int endSegment;


    public AimQuery(AimPartition table) { 
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

    public AimFilter filter() {
        return new AimFilter(table.schema);
    }

    public Pipe select(final String... colNames) throws IOException {
        final Pipe range = table.select(startSegment, endSegment, null, colNames);
        return new Pipe() {
            private int fieldIndex = -1;
            @Override public byte[] read(AimDataType type) throws IOException {
                if (++fieldIndex == colNames.length) fieldIndex = 0;
                return range.read(type);
            }
        };
    }

    public Pipe select(AimFilter filter, final String... colNames) throws IOException {
        return table.select(startSegment, endSegment, filter, colNames);
    }

    public Long count(AimFilter filter) throws IOException {
        return table.count(startSegment, endSegment, filter); 
    }

}
