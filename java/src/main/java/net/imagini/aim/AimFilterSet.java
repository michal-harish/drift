package net.imagini.aim;

import java.io.IOException;
import java.util.BitSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import net.imagini.aim.pipes.Pipe;

public class AimFilterSet extends ConcurrentHashMap<Integer,BitSet> {

    private static final long serialVersionUID = 1L;
    private Map<Integer,Long> lengths = new ConcurrentHashMap<>();

    public long cardinality() {
        long result = 0;
        for(BitSet segmentSet: this.values()) {
            result += segmentSet.cardinality();
        }
        return result;
    }
    public void length(Integer segment, Integer length) {
        lengths.put(segment, (long) length);
    }
    public Long length() {
        long result = 0;
        for(Integer segment: this.keySet()) {
            result += lengths.get(segment);
        }
        return result;
    }

    public Set<Integer> segments() {
        return this.keySet();
    }

    public void write(Pipe out) throws IOException {
        out.write("AIM".getBytes());                // HEADER [3]
        out.write((byte)100);                       // BYTE[1] connection type
        out.write(this.size());                     // INT num_segments
        for(Integer segment: this.keySet()) {       // ...
            out.write(segment);                     // INT segment_id
            out.write(lengths.get(segment));        // LONG bitset absolute bit length
            byte[] x = this.get(segment).toByteArray();
            out.write(x.length);                    // INT biteset serialized byte-length 
            out.write(x);
        }
    }

}
