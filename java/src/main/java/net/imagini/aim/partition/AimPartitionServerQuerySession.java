package net.imagini.aim.partition;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.imagini.aim.Pipe;
import net.imagini.aim.segment.AimFilter;
import net.imagini.aim.types.AimDataType;
import net.imagini.aim.types.AimSchema;
import net.imagini.aim.types.AimType;

import org.apache.commons.lang3.StringUtils;

public class AimPartitionServerQuerySession extends Thread {

    private Pipe pipe;
    private AimPartition table;

    public AimPartitionServerQuerySession(AimPartition table, Pipe pipe) throws IOException {
        this.pipe = pipe;
        this.table = table;
    }

    @Override
    public void run() {
        try {
            AimSchema schema = table.schema;
            AimQuery query = new AimQuery(table);
            Integer range = null;
            AimFilter filter = null;
            while(true) {
                String command;
                Queue<String> cmd;
                String input = pipe.read();
                try {
                    cmd = tokenize(input);
                    command = cmd.poll().toUpperCase();
                    switch(command.toUpperCase()) {

                        default: break;

                        case "STATS": handleStats(cmd); break;

                        case "ALL": range = null; filter = null; break;
                        case "LAST": range = table.getNumSegments()-1; filter = null; break;
                        case "RANGE": handleRangeQuery(cmd,query); filter = null; break;
                        case "FILTER": 
                            if (cmd.size()>0) {
                                filter = handleFilterQuery(range, query, schema, cmd); 
                            }
                            executeCount(range, query, filter);
                            break;

                        case "SELECT": 
                            if (cmd.size()>0) {
                                schema = table.schema.subset(new ArrayList<String>(cmd));
                            }
                            executeSelect(range, query, schema, filter, true); 
                            break;
                    }
                    pipe.write(false);
                    pipe.flush();
                } catch (Exception e) {
                    try {
                        pipe.write(true);
                        pipe.write("ERROR");
                        pipe.write(exceptionAsString(e));
                        pipe.write(false);
                        pipe.flush();
                    } catch (Exception e1) {
                        System.out.println("Could not send error response to the clien: ");
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e) {
            try {
                pipe.close();
            } catch (IOException e1) {}
            return;
        }
    }

    private void handleStats(Queue<String> cmd) throws IOException {
        pipe.write(true);
        pipe.write("STATS");
        pipe.write(table.schema.toString());
        pipe.write(table.getCount());
        pipe.write(table.getNumSegments());
        pipe.write(table.getSize());
        pipe.write(table.getOriginalSize());
    }

    private AimFilter handleFilterQuery(Integer range, AimQuery query, AimSchema schema, Queue<String> cmd) throws IOException {
        query.range(range);

        AimFilter rootFilter = query.filter();

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

    private AimQuery handleRangeQuery(Queue<String> cmd, AimQuery query) {
        // TODO Auto-generated method stub
        return null;
    }

    private void executeCount(
        Integer range, 
        AimQuery query, 
        AimFilter filter
    ) throws IOException, ExecutionException {
        long t = System.currentTimeMillis();
        t = (System.currentTimeMillis()-t);
        query.range(range);
        pipe.write(true);
        pipe.write("COUNT");
        Long count = query.count(filter);
        pipe.write(filter == null ? null : filter.toString());
        pipe.write(count);
        pipe.write((long)table.getCount());
        pipe.flush();
    }

    private void executeSelect(
        Integer range, 
        AimQuery query, 
        AimSchema schema, 
        AimFilter filter,
        boolean fetch
    ) throws IOException {
        query.range(range);
        Pipe result = query.select(filter, schema.names());
        pipe.write(true);
        pipe.write("RESULT");
        pipe.write(schema.toString());
        pipe.write(filter == null ? null : filter.toString());

        long count = 0; 
        try {
            boolean written;
            while(true) {
                written = false;
                for(AimType type: schema.fields()) {
                    AimDataType dataType = type.getDataType();
                    if (!fetch) {
                        result.skip(dataType);
                    } else {
                        byte[] val = result.read(dataType);
                        if (!written) pipe.write(written = true);
                        pipe.write(dataType.getDataType(), val);
                    }

                }
                count++;
            }
        } catch (Exception e) {
            pipe.write(false);
            pipe.write((long)count);
            pipe.write((long)table.getCount());
            pipe.write((e instanceof EOFException ? "" : exceptionAsString(e))); //success flag
            pipe.flush();
        }

    }

    static enum Token { 
        WHITESPACE,KEYWORD,OPERATOR,NUMBER,STRING
    }

    @SuppressWarnings("serial")
    static final Map<Token,Pattern> matchers = new HashMap<Token,Pattern>() {{
        put(Token.WHITESPACE, Pattern.compile("^((\\s+))"));
        put(Token.KEYWORD, Pattern.compile("^(([A-Za-z_]+))"));
        put(Token.OPERATOR, Pattern.compile("^(([\\!@\\$%\\^&\\*;\\:|,<.>/\\?\\-=\\+\\(\\)\\[\\]\\{\\}`~]+))")); // TODO define separately () {} []
        put(Token.NUMBER,  Pattern.compile("^(([0-9]+|[0-9]+\\.[0-9]+))"));
        put(Token.STRING, Pattern.compile("^('(.*?)')")); // TODO fix escape sequence (?:\\"|.)*? OR /'(?:[^'\\]|\\.)*'/
    }};

    private Queue<String> tokenize(String input) throws IOException {
        Queue<String> result = new LinkedList<String>();
        int i = 0;
        main: while(i<input.length()) {
            String s = input.substring(i);
            for(Entry<Token,Pattern> p: matchers.entrySet()) {
                Matcher m = p.getValue().matcher(s);
                if (m.find()) {
                    i += m.group(1).length();
                    String word = m.group(2);
                    if (!p.getKey().equals(Token.WHITESPACE)) {
                        result.add(word);
                    }
                    continue main;
                }
            }
            throw new IllegalArgumentException("Invalid query near: " + s);
        }
        return result;
    }

    public static String exceptionAsString(Exception e) {
        String[] trace = new String[e.getStackTrace().length];
        int i = 0; for(StackTraceElement t:e.getStackTrace()) trace[i++] = t.toString();
        return e.toString() + "\n"  + StringUtils.join(trace,"\n");
    }

}
