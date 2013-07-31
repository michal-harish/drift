package net.imagini.aim;

import java.io.IOException;
import java.nio.ByteBuffer;

import joptsimple.internal.Strings;
import net.imagini.aim.node.AimTable;
import net.imagini.aim.pipes.Pipe;

/**
 * AimFilter objects can be chained into a filter chain that is evaulated from left to right.
 * 
 * Example:
 * 
 *  AimTable table = ...
 *  AimFilter filter = new AimFilter(table);
 *      .where("timestamp").greaterThan("1374467639")
 *      .and("timestamp").lessThan("12746999");
 * 
 * The filter object can then be used as an argument to the table.open(..) method which
 * will return a virtual filtered InputStream for all the selected columns and all
 * records that match the filter criteria:
 * 
 *  table.open(..,filter,"timestamp","user_uid","api_key,"url");
 * 
 * 
 * @author mharis
 */
public class AimFilter {

    protected static AimFilter proxy(AimFilter root, AimTable table, String expression) {
        AimFilter result = new AimFilterSimple(root, table, expression);
        result.root = root;
        return result;
    }

    public AimFilter(AimTable table) {
        this(null, null);
        this.root = this;
        this.table = table;
    }

    private AimTable table; //TODO remove table and use schema only
//    private Integer startSegment;
//    private Integer endSegment;
    protected AimFilter root;
    protected AimType type;
    protected AimFilter next;

    protected AimFilter(AimFilter root, AimType type) {
        this(root,type,null);
    }

    protected AimFilter(AimFilter root, AimType type, AimFilter next) {
        this.type = type;
        this.root = root;
        this.next = next;
    }

    final public void updateFormula(String[] usedColumns) {
        root.update(usedColumns);
    }

    final public void write(Pipe pipe) throws IOException {
        pipe.write(root.toString());
    }

    @Override public String toString() {
        return (next != null) ? " " + next.toString() : "";
    }

    protected void update(String[] usedColumns) {
        if (next != null) next.update(usedColumns);
    }

    public boolean match(LZ4Buffer[] record) {
        return root.match(true, record);
    }

    protected boolean match(boolean soFar, LZ4Buffer[] record) {
        return next == null ? soFar : next.match(soFar, record);
    }

    protected boolean match( LZ4Buffer value, LZ4Buffer[] record) {
        throw new IllegalAccessError(this.getClass().getSimpleName() + " cannot be matched against a value");
    }

    public AimFilter where(String expression) {
        return next = AimFilter.proxy(root, root.table,expression);
    }

    public AimFilter equals(final String value) {
        if (type == null) return next.equals(value);
        final ByteBuffer val = ByteBuffer.wrap(type.convert(value));
        return next = new AimFilter(root,type) {
            @Override public String toString() { return "= " + type.wrap(value) + super.toString(); }
            @Override protected boolean match(LZ4Buffer value, LZ4Buffer[] record) {
                //FIXME with buffers the arrays have to be compared not checked for equality, probably use ByteArrayWrappers 
                return super.match(value.compare(val,type.getDataType())==0, record);
            }
        };
    }

    public AimFilter and(String expression) {
        return next = new AimFilterOp(root, expression) {
            @Override public String toString() { return "AND" + super.toString(); }
            @Override protected boolean compare(boolean a, boolean b) {
                return a & b;
            }
        };
    }

    public AimFilter or(String expression) {
        return next = new AimFilterOp(root, expression) {
            @Override public String toString() { return "OR"+ super.toString(); }
            @Override protected boolean compare(boolean a, boolean b) {
                return a | b;
            }
        };
    }

    public AimFilter not() {
        return next = new AimFilter(root, type, next) {
            @Override public String toString() { return "NOT" + super.toString(); }
            @Override protected boolean match(boolean soFar, LZ4Buffer[] record) {
                return !next.match(soFar, record);
            }
        };
    }

    public AimFilter contains(final String value) {
        if (type == null) return next.contains(value);
        final ByteBuffer val = ByteBuffer.wrap(type.convert(value));
        return next = new AimFilter(root,type) {
            @Override public String toString() { return "CONTAINS " + type.wrap(value) + super.toString(); }
            @Override protected boolean match(LZ4Buffer value,  LZ4Buffer[] record) {
                return super.match(value.contains(val,type.getDataType()), record);
            }
        };
    }


    public AimFilter greaterThan(final String value) {
        if (type == null) return next.greaterThan(value);
        final ByteBuffer val = ByteBuffer.wrap(type.convert(value));
        return next = new AimFilter(root,type) {
            @Override public String toString() { return "> " + type.wrap(value) + super.toString(); }
            @Override protected boolean match(LZ4Buffer value, LZ4Buffer[] data) {
                return super.match(value.compare(val,type.getDataType()) > 0, data);
            }
        };
    }

    public AimFilter lessThan(final String value) {
        if (type == null) return next.lessThan(value);
        final ByteBuffer val = ByteBuffer.wrap(type.convert(value));
        return next = new AimFilter(root,type) {
            @Override public String toString() { return "< " + type.wrap(value) +super.toString(); }
            @Override protected boolean match(LZ4Buffer value, LZ4Buffer[] data) {
                return super.match(value.compare(val,type.getDataType()) < 0, data);
            }
        };
    }

    public AimFilter in(final String... values) {
        if (type == null) return next.in(values);
        final ByteBuffer[] vals = new ByteBuffer[values.length]; 
        int i = 0; for(String value:values) {
            vals[i++] = ByteBuffer.wrap(type.convert(value));
        }
        return next = new AimFilter(root,type) {
            @Override public String toString() { return "IN (" + Strings.join(values, ",") +")" + super.toString(); }
            @Override protected boolean match(LZ4Buffer value, LZ4Buffer[] data) {
                boolean localResult = false;
                for(ByteBuffer val: vals) {
                    if (value.compare(val,type.getDataType())==0) {
                        localResult = true;
                        break;
                    }
                }
                return super.match(localResult, data);
            }
        };
    }

    public static class AimFilterSimple extends AimFilter {
        private int colIndex;
        private String colName;
        public AimFilterSimple(AimFilter root, AimTable table, String field) {
            super(root, table.def(field));
            this.colName = field;
        }
        @Override public void update(String[] usedColumns) {
            super.update(usedColumns);
            for(colIndex=0;colIndex<usedColumns.length;colIndex++) {
                if (usedColumns[colIndex].equals(colName)) break;
            }
            if (colIndex == -1) {
                throw new IllegalArgumentException("Unknwon filter column " + colName);
            }
        }
        @Override protected boolean match(boolean soFar, LZ4Buffer[] data) {
            return next.match(data[colIndex], data);
        }
        @Override public String toString() {
            return colName + super.toString();
        }

    }

    abstract static public class AimFilterOp extends AimFilter {
        public AimFilterOp(AimFilter root, String expression) {
            super(root, null);
            next = AimFilter.proxy(root, root.table,expression);
        }
        @Override
        protected boolean match(boolean soFar, LZ4Buffer[] data) {
            return compare(soFar, next.match(true, data));
        }
        abstract protected boolean compare(boolean a, boolean b);

    }

    /**
     * Experimental multi-threaded filter executor.
     * 
     * filters run for each segment in parallel thread and should be sending 
     * the serialized filter over the pipe "to" the segment.
     
    final public AimFilterSet go() throws IOException {
        final AimFilterSet result = new AimFilterSet();
        final List<Integer> segments = new LinkedList<Integer>();
        for(int s = root.startSegment; s <= root.endSegment; s++) segments.add(s);
        if (segments.size() == 0) return result;
        final ExecutorService collector = Executors.newFixedThreadPool(segments.size());

        for(final Integer s : segments) {
            final AimSegment segment = root.table.open(s);
            if (segment == null) return result;
            collector.submit(new Runnable() {
                @Override
                public void run() {
                    BitSet segmentResult = new BitSet();
                    try {
                        int length = segment.filter(root, segmentResult);
                        result.put(s, segmentResult);
                        result.length(s, length);
                    } catch (IOException e) {
                        result.put(s, null);
                        e.printStackTrace();
                        collector.shutdownNow();
                    }
                }
            });
        }
        collector.shutdown();
        try {
            collector.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }

        return result;
    }
*/

}
