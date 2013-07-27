package net.imagini.aim;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import net.imagini.aim.node.AimRecord;
import net.imagini.aim.node.AimTable;

/**
 * @author mharis
 */
abstract public class AimFilter {

    public static AimFilter proxy(AimFilter root, AimTable table, String expression) {
        AimFilter result = new AimFilterSimple(root, table, expression);
        result.root = root;
        return result;
    }
    public static AimFilter proxy(AimTable table, Integer startSegment, Integer  endSegment, String expression) {
        AimFilter result = proxy(null, table,  expression);
        result.table = table;
        result.startSegment = startSegment;
        result.endSegment = endSegment;
        result.root = result;
        return result;
    }

    private AimTable table;
    private Integer startSegment;
    private Integer endSegment;
    protected AimFilter root;
    protected AimType type;
    protected AimFilter next;

    public AimFilter(AimFilter root, AimType type) {
        this(root,type,null);
    }

    public AimFilter(AimFilter root, AimType type, AimFilter next) {
        this.type = type;
        this.root = root;
        this.next = next;
    }

    final public void updateFormula(List<String> usedColumns) {
        root.update(usedColumns);
    }
    
    protected void update(List<String> usedColumns) {
        if (next != null) next.update(usedColumns);
    }

    final public boolean match(AimRecord record) {
        // TODO Auto-generated method stub
        return false;
    }
    public boolean match(byte[][] data) {
        return root.match(true, data);
    }

    protected boolean match(boolean soFar, byte[][] data) {
        return next == null ? soFar : next.match(soFar, data);
    }

    protected boolean match( byte[] value, byte[][] data) {
        throw new IllegalAccessError(this.getClass().getSimpleName() + " cannot be matched against a value");
    }

    public AimFilter and(String expression) {
        return next = new AimFilterOp(root, expression) {
            @Override protected boolean compare(boolean a, boolean b) {
                return a & b;
            }
        };
    }

    public AimFilter or(String expression) {
        return next = new AimFilterOp(root, expression) {
            @Override protected boolean compare(boolean a, boolean b) {
                return a | b;
            }
        };
    }

    public AimFilter not() {
        return next = new AimFilter(root, type, next) {
            @Override protected boolean match(boolean soFar, byte[][] data) {
                return !next.match(soFar, data);
            }
        };
    }

    public AimFilter equals(String value) {
        if (type == null) return next.equals(value);
        final byte[] val = type.convert(value);
        return next = new AimFilter(root,type) {
            @Override protected boolean match(byte[] value, byte[][] data) {
                return super.match(Arrays.equals(value, val), data);
            }
        };
    }
    public AimFilter contains(String value) {
        if (type == null) return next.contains(value);
        final byte[] val = type.convert(value);
        return next = new AimFilter(root,type) {
            @Override protected boolean match(byte[] value, byte[][] data) {
                int v = 0;for(byte b: val) {
                    if (val[v]!=b) v = 0;
                    else if (++v==val.length) {
                        return super.match(true, data);
                    }
                }
                return super.match(false, data);
            }
        };
    }


    public AimFilter greaterThan(final String than) {
        if (type == null) return next.greaterThan(than);
        final byte[][] vals = new byte[1][];
        vals[0] = type.convert(than);
        return next = new AimFilter(root,type) {
            @Override protected boolean match(byte[] value, byte[][] data) {
                int[] cmp = AimFilter.compare(value, vals);
                return super.match(cmp[0] == 1, data);
            }
        };
    }

    public AimFilter lessThan(final String than) {
        if (type == null) return next.lessThan(than);
        final byte[][] vals = new byte[1][];
        vals[0] = type.convert(than);
        return next = new AimFilter(root,type) {
            @Override protected boolean match(byte[] value, byte[][] data) {
                int[] cmp = AimFilter.compare(value, vals);
                return super.match(cmp[0] == -1, data);
            }
        };
    }

    public AimFilter in(String... values) {
        if (type == null) return next.in(values);
        final byte[][] vals = new byte[values.length][]; 
        int i = 0; for(String value:values) {
            vals[i++] = type.convert(value);
        }
        return next = new AimFilter(root,type) {
            @Override
            protected boolean match(byte[] value, byte[][] data) {
                boolean localResult = false;
                for(byte[] val: vals) {
                    if (Arrays.equals(value, val)) {
                        localResult = true;
                        break;
                    }
                }
                return super.match(localResult, data);
            }
        };
    }

    /**
     * filters run for each segment in parallel thread and should be sending 
     * the serialized filter over the pipe "to" the segment.
     */
    final public AimFilterSet go() throws IOException {
        final AimFilterSet result = new AimFilterSet();
        final List<Integer> segments = new LinkedList<Integer>();
        for(int s = root.startSegment; s <= root.endSegment; s++) segments.add(s);
        final ExecutorService collector = Executors.newFixedThreadPool(segments.size());

        for(final Integer s : segments) {
            final AimSegment segment = root.table.open(s);
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

    public static class AimFilterSimple extends AimFilter {
        private int colIndex;
        private String colName;
        public AimFilterSimple(AimFilter root, AimTable table, String field) {
            super(root, table.def(field));
            this.colName = field;
        }
        @Override public void update(List<String> usedColumns) {
            super.update(usedColumns);
            colIndex = usedColumns.indexOf(colName);
        }
        @Override protected boolean match(boolean soFar, byte[][] data) {
            return next.match(data[colIndex], data);
        }
        @Override public String toString() {
            return "field(" +colName + ")";
        }

    }

    abstract static public class AimFilterOp extends AimFilter {
        public AimFilterOp(AimFilter root, String expression) {
            super(root, null);
            next = AimFilter.proxy(root, root.table,expression);
        }
        @Override
        protected boolean match(boolean soFar, byte[][] data) {
            return compare(soFar, next.match(true, data));
        }
        abstract protected boolean compare(boolean a, boolean b);
    }

    static public int[] compare(byte[] data, byte[][] vals) {
        int[] result = new int[vals.length];
        Arrays.fill(result, 0);
        int i = 0;
        while (i<data.length) {
            int r = -1;
            for(byte[] val: vals) if (result[++r] == 0) {
                if (data.length < val.length) {
                    result[r] = -1;
                } else if (data.length > val.length) {
                    result[r] = 1;
                } else if ((data[i] & 0xFF) < (val[i] & 0xFF)) {
                    result[r] = -1;
                } else if ((data[i] & 0xFF) > (val[i] & 0xFF) ) {
                    result[r] = 1;
                }
            }
            i++;
        }
        return result;
    }

}
