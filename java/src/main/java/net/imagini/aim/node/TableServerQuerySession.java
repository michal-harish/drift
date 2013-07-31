package net.imagini.aim.node;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import joptsimple.internal.Strings;
import net.imagini.aim.AimFilter;
import net.imagini.aim.AimQuery;
import net.imagini.aim.AimSchema;
import net.imagini.aim.AimType;
import net.imagini.aim.AimTypeAbstract.AimDataType;
import net.imagini.aim.pipes.Pipe;

public class TableServerQuerySession extends Thread {

    private Pipe pipe;
    private AimTable table;

    public TableServerQuerySession(AimTable table, Pipe pipe) throws IOException {
        this.pipe = pipe;
        this.table = table;
    }

    @Override
    public void run() {
        try {
            AimSchema schema = table.schema.subset("user_uid","user_quizzed","api_key","timestamp","post_code");//,"url"
            AimQuery query = new AimQuery(table);
            Integer range = null;
            AimFilter filter = query.filter().where("user_quizzed").equals("true");
            while(true) {
                String input = pipe.read();
                Queue<String> cmd = tokenize(input);
                String command = cmd.poll().toUpperCase();
                switch(command.toUpperCase()) {

                    case "STATS": handleStats(cmd); break;
                    case "SCAN": handleScan(cmd); break;

                    case "ALL": range = null; filter = null; break;
                    case "LAST": range = table.getNumSegments()-1; filter = null; break;
                    case "RANGE": handleRangeQuery(cmd,query); filter = null; break;

                    case "FILTER": 
                        if (cmd.size()>0) {
                            filter = handleFilterQuery(range, query, schema, cmd); 
                        }
                        executeQuery(range, query, schema, filter, false);
                        break;

                    case "SELECT": 
                        if (cmd.size()>0) {
                            schema = table.schema.subset(new ArrayList<String>(cmd));
                        }
                        executeQuery(range, query, schema, filter, true); 
                        break;

                    default: 
                        pipe.write(true);
                        pipe.write("ERROR");
                        pipe.write("Unknown query command " + command); 
                        break;
                }
                pipe.write(false);
                pipe.flush();
            }
        } catch (IOException e) {
            try {
                pipe.write(true);
                pipe.write("ERROR");
                pipe.write("Unknown command " + e.getMessage()).flush();
                pipe.write(false);
                pipe.flush();
            } catch (Exception e1) {
                System.out.println("Could not send error response to the clien: ");
                e.printStackTrace();
            }
        }
    }
    private void handleStats(Queue<String> cmd) throws IOException {
        pipe.write(true);
        pipe.write("STATS");
        pipe.write(table.name);
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
                            filter = filter.in(Strings.join(values, ","));
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

    private void handleScan(Queue<String> cmd) throws IOException {
        AimSchema schema = table.schema.subset(Arrays.asList("timestamp","api_key","user_quizzed","user_uid"));
        long t = System.currentTimeMillis();
        int n = table.getNumSegments();
        Pipe scanner = table.open(0, n-1, null, schema.names());
        System.err.println("Open ms: " + (System.currentTimeMillis()-t));
        long count = 0;
        table.loadRecordsMs = 0;
        table.readMs = 0;
        table.mergeSortMs = 0;
        try {
            while(true) {
                for (AimType type : schema.def()) {
                    scanner.read(type.getDataType());
                }
                count++;
            }
        } catch (EOFException e) {
            System.err.println("Segment scanned records: "+count);
            System.err.println("-nextRecord ms: " + table.loadRecordsMs);
            System.err.println("--table.mergeSortMs ms: " + table.mergeSortMs);
            System.err.println("--table.readMs ms: " + table.readMs);
        }
    }

    private void executeQuery(
        Integer range, 
        AimQuery query, 
        AimSchema schema, 
        AimFilter filter,
        boolean fetch
    ) throws IOException {
        long t = System.currentTimeMillis();
        t = (System.currentTimeMillis()-t);
        query.range(range);
        Pipe result = query.select(filter, schema.names());
        pipe.write(true);
        pipe.write("RESULT");
        pipe.write(schema.toString());
        pipe.write(filter.toString());
        long count = 0; 
        try {
            boolean written;
            while(true) {
                written = false;
                for(AimType type: schema.def()) {
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
            pipe.write((e instanceof EOFException ? "" : e.getMessage() + ": "+ e.getStackTrace()[0].toString())); //success flag
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

}
