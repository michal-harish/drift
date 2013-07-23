package net.imagini.aim;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import net.imagini.aim.pipes.Pipe;


abstract public class AimFilter {

    public static AimFilter proxy(AimFilter prev, AimTable table, String expression) {
        AimFilter result = proxy(table, null, null, expression);
        result.prev = prev;
        return result;
    }
    public static AimFilter proxy(AimTable table, Integer startSegment, Integer  endSegment, String expression) {
        AimFilter result = new AimFilterSimple(null, table, expression);
        result.startSegment = startSegment;
        result.endSegment = endSegment;
        return result;
    }

    private AimTable table;
    private Integer startSegment;
    private Integer endSegment;
    protected AimFilter prev;
    protected AimDataType type;
    protected AimFilter next;

    public AimFilter(AimFilter prev, AimTable table, AimDataType type) {
        this.table = table;
        this.type = type;
        this.prev = prev;
    }

    final protected AimFilter start() {
        return prev == null ? this : prev.start();
    }

    protected void updateFormula(LinkedHashMap<String,AimDataType> def) {
        if (prev != null) prev.updateFormula(def);
    }

    protected boolean match(boolean soFar, byte[][] data) {
        return next == null ? soFar : next.match(soFar, data);
    }

    protected boolean match( byte[] value, byte[][] data) {
        throw new IllegalAccessError(this.getClass().getSimpleName() + " cannot be matched against a value");
    }

    public AimFilter eq(String value) {
        if (type == null) return next.eq(value);
        final byte[] val = Pipe.convert(type, value);
        return next = new AimFilter(this,table,type) {
            @Override protected boolean match(byte[] value, byte[][] data) {
                return super.match(Arrays.equals(value, val), data);
            }
        };
    } 
    public AimFilter in(String... values) {
        if (type == null) return next.in(values);
        final byte[][] vals = new byte[values.length][]; 
        int i = 0; for(String value:values) {
            vals[i++] = Pipe.convert(type, value);
        }
        return next = new AimFilter(this,table,type) {
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
    public AimFilter greater(final String than) {
        if (type == null) return next.greater(than);
        final byte[][] vals = new byte[1][];
        vals[0] = Pipe.convert(type, than);
        return next = new AimFilter(this,table,type) {
            @Override protected boolean match(byte[] value, byte[][] data) {
                int[] cmp = AimFilter.compare(value, vals);
                return super.match(cmp[0] == 1, data);
            }
        };
    }
    public AimFilter smaller(final String than) {
        if (type == null) return next.smaller(than);
        final byte[][] vals = new byte[1][];
        vals[0] = Pipe.convert(type, than);
        return next = new AimFilter(this,table,type) {
            @Override protected boolean match(byte[] value, byte[][] data) {
                int[] cmp = AimFilter.compare(value, vals);
                return super.match(cmp[0] == -1, data);
            }
        };
    }


    public AimFilter and(String expression) {
        return next = new AimFilterOp(this, table, expression) {
            @Override protected boolean compare(boolean a, boolean b) {
                return a & b;
            }
        };
    }

    public AimFilter or(String expression) {
        return next = new AimFilterOp(this, table, expression) {
            @Override protected boolean compare(boolean a, boolean b) {
                return a | b;
            }
        };
    }

    @SuppressWarnings("serial")
    final public BitSet go() throws IOException {
        LinkedHashMap<String,AimDataType> cols = new LinkedHashMap<String,AimDataType>() {{
            put("userQuizzed",Aim.BOOL);
            put("post_code",Aim.STRING);
            put("api_key",Aim.STRING);
            put("timestamp",Aim.LONG);
        }};
        //TODO dig colNames
        updateFormula(cols);
        AimFilter start = start();
        final AimTable table = start.table;
        final String[] colNames = cols.keySet().toArray(new String[cols.size()]);
        final AimDataType[] types = new LinkedList<AimDataType>() {{
            for(String colName: colNames) add(table.def(colName));
        }}.toArray(new AimDataType[colNames.length]);
        byte[][] data = new byte[colNames.length][];
        
        //TODO run filters for each segment in parallel thread so don't use table.range(..)
        //i.e. don't rely on ColumnInputStream but operate on the raw SegmentInputStream
        InputStream[] range = table.range(start.startSegment, start.endSegment, colNames);
        BitSet result = new BitSet(); 
        int record = 0;
        try {
            while (true) {
                for(int i=0;i<types.length;i++) {
                    data[i] = Pipe.read(range[i],types[i]);
                }
                if (start.match(true,data)) {
                    result.set(record);
                }
                record++;
            }
        } catch (EOFException e) {}
        return result;
    }

    public static class AimFilterSimple extends AimFilter {
        private int colIndex;
        private String colName;
        public AimFilterSimple(AimFilter prev, AimTable table, String field) {
            super(prev, table, table.def(field));
            this.colName = field;
        }
        @Override protected void updateFormula(LinkedHashMap<String,AimDataType> def) {
            super.updateFormula(def);
            int i=0; for(Entry<String,AimDataType> col: def.entrySet()) {
                if (col.getKey().equals(colName)) {
                    colIndex = i;
                }
                i++;
            }
        }
        @Override protected boolean match(boolean soFar, byte[][] data) {
            return next.match(data[colIndex], data);
        }
        @Override public String toString() {
            return "field(" +colName + ")";
        }

    }
    
    abstract static public class AimFilterOp extends AimFilter {
        public AimFilterOp(AimFilter prev, AimTable table, String expression) {
            super(prev, table, null);
            next = AimFilter.proxy(prev,table,expression);
        }
        @Override
        protected boolean match(boolean soFar, byte[][] data) {
            boolean localResult = compare(soFar, next.match(true, data));
            return localResult;
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
