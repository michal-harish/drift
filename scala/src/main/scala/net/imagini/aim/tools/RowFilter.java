package net.imagini.aim.tools;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import net.imagini.aim.cluster.Pipe;
import net.imagini.aim.types.AimSchema;
import net.imagini.aim.types.AimType;
import net.imagini.aim.types.TypeUtils;
import net.imagini.aim.utils.ByteUtils;
import net.imagini.aim.utils.Tokenizer;

import org.apache.commons.lang3.StringUtils;

/**
 * RowFilter objects can be chained into a filter chain that is evaulated from left to right.
 * 
 * Example:
 * 
 * RowFilter filter = new RowFilter(schema);
 *      .where("timestamp").greaterThan("1374467639")
 *      .and("timestamp").lessThan("12746999");
 *
 * Alternative:
 *
 * RowFilter.fromString("timestamp > 1374467639 and timestamp < 12746999")
 *
 * @author mharis
 */

public class RowFilter {

    public static RowFilter fromString(AimSchema schema, String declaration) {
        return fromTokenQueue(schema, Tokenizer.tokenize(declaration));
    }

    public static RowFilter fromTokenQueue(AimSchema schema, Queue<String> cmd) {
        RowFilter rootFilter = new RowFilter(schema);
        RowFilter filter = rootFilter;
        while(cmd.size()>0) {
            String subject = cmd.poll();
            switch(subject.toUpperCase()) {
                case "*": rootFilter.isEmptyFilter = true; return rootFilter; 
                case "AND": filter = filter.and(cmd.poll()); break;
                case "OR": filter = filter.or(cmd.poll()); break;
                default: filter = filter.where(subject); break; //expression
            }
            op: while(cmd.size()>0) {
                String predicate = cmd.poll();
                switch(predicate.toUpperCase()) {
                    case "NOT": filter = filter.not(); continue op;
                    case "IN":
                        if (cmd.poll().equals("(")) {
                            List<String> values = new ArrayList<String>();
                            String value;
                            while (!")".equals(value = cmd.poll())) {
                                if (value.equals(",")) value = cmd.poll();
                                values.add(value);
                            }
                            filter = filter.in(values.toArray(new String[values.size()]));
                        }
                        break;
                    case "CONTAINS": filter = filter.contains(cmd.poll()); break;
                    case "=": filter = filter.equals(cmd.poll()); break;
                    case ">": filter = filter.greaterThan(cmd.poll()); break;
                    case "<": filter = filter.lessThan(cmd.poll()); break;
                    default:break;
                }
                break;
            }
        }
        return rootFilter;
    }

    protected static RowFilter proxy(RowFilter root, AimSchema schema, String expression) {
        RowFilter result = new RowFilterSimple(root, schema, expression);
        result.root = root;
        return result;
    }

    private AimSchema schema;
    protected RowFilter root;
    protected AimType aimType;
    protected RowFilter next;
    protected boolean isEmptyFilter = false;
    public boolean isEmptyFilter() {
        return isEmptyFilter;
    }


    public RowFilter(AimSchema schema) {
        this(null, null);
        this.root = this;
        this.schema = schema;
    }

    public AimSchema schema() {
        return root.schema;
    }

    protected RowFilter(RowFilter root, AimType aimType) {
        this(root,aimType,null);
    }

    protected RowFilter(RowFilter root, AimType type, RowFilter next) {
        this.aimType = type;
        this.root = root;
        this.next = next;
    }

    /**
     * This is thread-safe and called in parallel for 
     * multiple segments.
     */
    public boolean matches(ByteBuffer[] record) {
        return root.matches(true, record);
    }

    /**
     * This has to be synchronized as it can be called from multiple
     * segments at the same time and changes some internal data.
     * It is not called frequently so it should be ok.
     */
    final synchronized public void updateFormula(String[] usedColumns) {
        root.update(usedColumns);
    }

    public String[] getColumns() {
        Set<String> fieldSet = root.getColumnSet();
        return fieldSet.toArray(new String[fieldSet.size()]);
    }

    protected Set<String> getColumnSet(String... fields) {
        Set<String >result = new HashSet<String>(Arrays.asList(fields));
        if (next != null) {
            result.addAll(next.getColumnSet());
        }
        return result;
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

    protected boolean matches(boolean soFar, ByteBuffer[] record) {
        return next == null ? soFar : next.matches(soFar, record);
    }

    protected boolean matches( ByteBuffer value, ByteBuffer[] record) {
        throw new IllegalAccessError(this.getClass().getSimpleName() + " cannot be matched against a value");
    }

    public RowFilter where(String expression) {
        return next = RowFilter.proxy(root, root.schema, expression);
    }

    public RowFilter equals(final String value) {
        if (aimType == null) return next.equals(value);
        final ByteBuffer val = ByteUtils.wrap(aimType.convert(value));
        return next = new RowFilter(root,aimType) {
            @Override public String toString() { return "= " + aimType.escape(value) + super.toString(); }
            @Override protected boolean matches(ByteBuffer value, ByteBuffer[] record) {
                boolean match = super.matches(TypeUtils.compare(value,val,aimType)==0, record);
                return match;
            }
        };
    }

    public RowFilter and(String expression) {
        return next = new RowFilterOp(root, expression) {
            @Override public String toString() { return "AND" + super.toString(); }
            @Override protected boolean compare(boolean a, boolean b) {
                return a & b;
            }
        };
    }

    public RowFilter or(String expression) {
        return next = new RowFilterOp(root, expression) {
            @Override public String toString() { return "OR"+ super.toString(); }
            @Override protected boolean compare(boolean a, boolean b) {
                return a | b;
            }
        };
    }

    public RowFilter not() {
        return next = new RowFilter(root, aimType, next) {
            @Override public String toString() { return "NOT" + super.toString(); }
            @Override protected boolean matches(boolean soFar, ByteBuffer[] record) {
                return !next.matches(soFar, record);
            }
        };
    }

    public RowFilter contains(final String value) {
        if (aimType == null) return next.contains(value);
        final ByteBuffer val = ByteUtils.wrap(aimType.convert(value));
        return next = new RowFilter(root,aimType) {
            @Override public String toString() { return "CONTAINS " + aimType.escape(value) + super.toString(); }
            @Override protected boolean matches(ByteBuffer value,  ByteBuffer[] record) {
                return super.matches(TypeUtils.contains(value, val,aimType.getDataType()), record);
            }
        };
    }

    public RowFilter greaterThan(final String value) {
        if (aimType == null) return next.greaterThan(value);
        final ByteBuffer val = ByteUtils.wrap(aimType.convert(value));
        return next = new RowFilter(root,aimType) {
            @Override public String toString() { return "> " + aimType.escape(value) + super.toString(); }
            @Override protected boolean matches(ByteBuffer value, ByteBuffer[] data) {
                return super.matches(TypeUtils.compare(value,val,aimType) > 0, data);
            }
        };
    }

    public RowFilter lessThan(final String value) {
        if (aimType == null) return next.lessThan(value);
        final ByteBuffer val = ByteUtils.wrap(aimType.convert(value));
        return next = new RowFilter(root,aimType) {
            @Override public String toString() { return "< " + aimType.escape(value) +super.toString(); }
            @Override protected boolean matches(ByteBuffer value, ByteBuffer[] data) {
                return super.matches(TypeUtils.compare(value, val,aimType) < 0, data);
            }
        };
    }

    public RowFilter in(final String... values) {
        if (aimType == null) return next.in(values);
        final ByteBuffer[] vals = new ByteBuffer[values.length]; 
        int i = 0; for(String value:values) {
            vals[i++] = ByteUtils.wrap(aimType.convert(value));
        }
        return next = new RowFilter(root,aimType) {
            @Override public String toString() { return "IN (" + StringUtils.join(values, ",") +")" + super.toString(); }
            @Override protected boolean matches(ByteBuffer value, ByteBuffer[] data) {
                boolean localResult = false;
                for(ByteBuffer val: vals) {
                    if (TypeUtils.compare(value, val,aimType)==0) {
                        localResult = true;
                        break;
                    }
                }
                return super.matches(localResult, data);
            }
        };
    }

    public static class RowFilterSimple extends RowFilter {
        private int colIndex;
        private String colName;
        public RowFilterSimple(RowFilter root, AimSchema schema, String field) {
            super(root, schema.field(field));
            this.colName = field;
        }
        @Override protected Set<String> getColumnSet(String... fields) {
            return super.getColumnSet(colName);
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
        @Override protected boolean matches(boolean soFar, ByteBuffer[] data) {
            return next.matches(data[colIndex], data);
        }
        @Override public String toString() {
            return colName + super.toString();
        }

    }

    abstract static public class RowFilterOp extends RowFilter {
        public RowFilterOp(RowFilter root, String expression) {
            super(root, null);
            next = RowFilter.proxy(root, root.schema,expression);
        }
        @Override
        protected boolean matches(boolean soFar, ByteBuffer[] data) {
            return compare(soFar, next.matches(true, data));
        }
        abstract protected boolean compare(boolean a, boolean b);
    }

}
