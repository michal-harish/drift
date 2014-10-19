package net.imagini.aim.partition;

import java.io.IOException;

import net.imagini.aim.segment.AimFilter;
import net.imagini.aim.tools.Pipe;
import net.imagini.aim.types.AimDataType;

/**
 * @author mharis
 */
public class AimQuery {

    private AimPartition partition;
    private int startSegment;
    private int endSegment;


    public AimQuery(AimPartition table) { 
        this.partition = table;
    }

    public void range(Integer segmentRange) {
        int segmentCount = partition.getNumSegments();
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
        return new AimFilter(partition.schema);
    }

    public Pipe select(final String... colNames) throws IOException {
        final Pipe range = partition.select(startSegment, endSegment, null, colNames);
        return new Pipe() {
            private int fieldIndex = -1;
            @Override public byte[] read(AimDataType type) throws IOException {
                if (++fieldIndex == colNames.length) fieldIndex = 0;
                return range.read(type);
            }
        };
    }

    public Pipe select(AimFilter filter, final String... colNames) throws IOException {
        return partition.select(startSegment, endSegment, filter, colNames);
    }

    public Long count(AimFilter filter) throws IOException {
        return partition.count(startSegment, endSegment, filter); 
    }

}
