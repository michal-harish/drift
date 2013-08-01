package net.imagini.aim;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    public AimFilter filter() {
        return new AimFilter(table);
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
        return table.open(startSegment, endSegment, filter, colNames);
    }

    final ExecutorService executor = Executors.newFixedThreadPool(4);

    public Long count(AimFilter filter) throws ExecutionException {
        return table.count(executor, startSegment, endSegment, filter); 
    }

}
