package net.imagini.aim;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicInteger;

public class AimQuery {

    private AimTable table;
    private Integer segmentCount;
    private Integer partitionRange;

    public AimQuery(AimTable table) {
        this(table, null);
    }

    public AimQuery(AimTable table, Integer segmentRange) { 
        this.table = table;
        this.segmentCount = table.getNumSegments();
        this.partitionRange = segmentRange;
    }

    public AimFilter filter(String colName) throws IOException {
        AimColumn column = table.column(colName);
        return new AimFilter(getColumnRange(column), column.type);
    }

    public Pipe[] select(String... colNames) throws IOException {
        Pipe[] result = new Pipe[colNames.length];
        int i = 0; for(String colName: colNames) {
            AimColumn column = table.column(colName);
            result[i++] = new Pipe(getColumnRange(column));
        }
        return result;
    }

    public Pipe[] select(final BitSet filter, String... colNames) throws IOException {
        Pipe[] result = new Pipe[colNames.length];
        int j = 0; for(String colName: colNames) {
            AimColumn column = table.column(colName);
            InputStream range = getColumnRange(column);
            result[j++] = new Pipe(range) {
                private AtomicInteger index = new AtomicInteger(0);
                private AtomicInteger remaining = new AtomicInteger(filter.cardinality()); 
                @Override public byte[] read(AimDataType type) throws IOException {
                    if (remaining.decrementAndGet() < 0)  throw new EOFException();
                    while (!filter.get(index.getAndIncrement())) super.skip(type);
                    return super.read(type);
                }
            };
        }
        return result;
    }

    private InputStream getColumnRange(AimColumn column) throws IOException {
        if (partitionRange == null) { //all segments
            return column.range(0, segmentCount-1);
        } else if (partitionRange < 0 ) { // latest -n segments
            return column.range(segmentCount-partitionRange, segmentCount-1);
        } else {// single specific segment
            return column.range(partitionRange, partitionRange);
        }
    }


}
