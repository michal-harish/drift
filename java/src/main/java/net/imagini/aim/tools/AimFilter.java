package net.imagini.aim.tools;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import net.imagini.aim.types.AimSchema;
import net.imagini.aim.types.AimType;

import org.apache.commons.lang3.StringUtils;

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

    public static AimFilter fromString(AimSchema schema, String declaration) {
        return fromTokenQueue(schema, Tokenizer.tokenize(declaration));
    }

    public static AimFilter fromTokenQueue(AimSchema schema, Queue<String> cmd) {
        //TODO if cmd == ('*') then return emptyFilter
        AimFilter rootFilter = new AimFilter(schema);
        AimFilter filter = rootFilter;
        while(cmd.size()>0) {
            String subject = cmd.poll();
            switch(subject.toUpperCase()) {
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

    protected static AimFilter proxy(AimFilter root, AimSchema schema, String expression) {
        AimFilter result = new AimFilterSimple(root, schema, expression);
        result.root = root;
        return result;
    }

    public AimFilter(AimSchema schema) {
        this(null, null);
        this.root = this;
        this.schema = schema;
    }

    private AimSchema schema;
    protected AimFilter root;
    protected AimType aimType;
    protected AimFilter next;

    protected AimFilter(AimFilter root, AimType aimType) {
        this(root,aimType,null);
    }

    protected AimFilter(AimFilter root, AimType type, AimFilter next) {
        this.aimType = type;
        this.root = root;
        this.next = next;
    }

    /**
     * This is thread-safe and called in parallel for 
     * multiple segments.
     */
    public boolean matches(Scanner[] record) {
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

    protected boolean matches(boolean soFar, Scanner[] record) {
        return next == null ? soFar : next.matches(soFar, record);
    }

    protected boolean matches( Scanner value, Scanner[] record) {
        throw new IllegalAccessError(this.getClass().getSimpleName() + " cannot be matched against a value");
    }

    public AimFilter where(String expression) {
        return next = AimFilter.proxy(root, root.schema, expression);
    }

    public AimFilter equals(final String value) {
        if (aimType == null) return next.equals(value);
        final ByteBuffer val = ByteBuffer.wrap(aimType.convert(value));
        return next = new AimFilter(root,aimType) {
            @Override public String toString() { return "= " + aimType.wrap(value) + super.toString(); }
            @Override protected boolean matches(Scanner value, Scanner[] record) {
                boolean match = super.matches(value.compare(val,aimType.getDataType())==0, record);
                return match;
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
        return next = new AimFilter(root, aimType, next) {
            @Override public String toString() { return "NOT" + super.toString(); }
            @Override protected boolean matches(boolean soFar, Scanner[] record) {
                return !next.matches(soFar, record);
            }
        };
    }

    public AimFilter contains(final String value) {
        if (aimType == null) return next.contains(value);
        final ByteBuffer val = ByteBuffer.wrap(aimType.convert(value));
        return next = new AimFilter(root,aimType) {
            @Override public String toString() { return "CONTAINS " + aimType.wrap(value) + super.toString(); }
            @Override protected boolean matches(Scanner value,  Scanner[] record) {
                return super.matches(value.contains(val,aimType.getDataType()), record);
            }
        };
    }

    public AimFilter greaterThan(final String value) {
        if (aimType == null) return next.greaterThan(value);
        final ByteBuffer val = ByteBuffer.wrap(aimType.convert(value));
        return next = new AimFilter(root,aimType) {
            @Override public String toString() { return "> " + aimType.wrap(value) + super.toString(); }
            @Override protected boolean matches(Scanner value, Scanner[] data) {
                return super.matches(value.compare(val,aimType.getDataType()) > 0, data);
            }
        };
    }

    public AimFilter lessThan(final String value) {
        if (aimType == null) return next.lessThan(value);
        final ByteBuffer val = ByteBuffer.wrap(aimType.convert(value));
        return next = new AimFilter(root,aimType) {
            @Override public String toString() { return "< " + aimType.wrap(value) +super.toString(); }
            @Override protected boolean matches(Scanner value, Scanner[] data) {
                return super.matches(value.compare(val,aimType.getDataType()) < 0, data);
            }
        };
    }

    public AimFilter in(final String... values) {
        if (aimType == null) return next.in(values);
        final ByteBuffer[] vals = new ByteBuffer[values.length]; 
        int i = 0; for(String value:values) {
            vals[i++] = ByteBuffer.wrap(aimType.convert(value));
        }
        return next = new AimFilter(root,aimType) {
            @Override public String toString() { return "IN (" + StringUtils.join(values, ",") +")" + super.toString(); }
            @Override protected boolean matches(Scanner value, Scanner[] data) {
                boolean localResult = false;
                for(ByteBuffer val: vals) {
                    if (value.compare(val,aimType.getDataType())==0) {
                        localResult = true;
                        break;
                    }
                }
                return super.matches(localResult, data);
            }
        };
    }

    public static class AimFilterSimple extends AimFilter {
        private int colIndex;
        private String colName;
        public AimFilterSimple(AimFilter root, AimSchema schema, String field) {
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
        @Override protected boolean matches(boolean soFar, Scanner[] data) {
            return next.matches(data[colIndex], data);
        }
        @Override public String toString() {
            return colName + super.toString();
        }

    }

    abstract static public class AimFilterOp extends AimFilter {
        public AimFilterOp(AimFilter root, String expression) {
            super(root, null);
            next = AimFilter.proxy(root, root.schema,expression);
        }
        @Override
        protected boolean matches(boolean soFar, Scanner[] data) {
            return compare(soFar, next.matches(true, data));
        }
        abstract protected boolean compare(boolean a, boolean b);

    }
}
