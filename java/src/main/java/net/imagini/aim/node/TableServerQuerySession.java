package net.imagini.aim.node;

import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.Queue;

import net.imagini.aim.AimFilter;
import net.imagini.aim.AimFilterSet;
import net.imagini.aim.AimQuery;
import net.imagini.aim.AimSchema;
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
            AimSchema schema = table.schema.subset("user_uid","user_quizzed","timestamp","post_code","url");
            AimQuery query = new AimQuery(table);
            Integer range = null;
            AimFilter filter = null;
            while(true) {
                Queue<String> cmd = parse(pipe.read());
                switch(cmd.poll().toUpperCase()) {

                    case "STATS": handleStats(cmd); break;

                    case "ALL": range = null; filter = null; break;
                    case "LAST": range = table.getNumSegments()-1; filter = null; break;
                    case "RANGE": handleRangeQuery(cmd,query); filter = null; break;
                    case "FILTER": filter = handleFilterQuery(range, query, cmd); break;
                    case "SELECT": handleSelect(range, query, schema, filter, cmd); break;
                        
                    default: 
                        pipe.write(true);
                        pipe.write("ERROR");
                        pipe.write("Unknown command " + cmd).flush(); 
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

    private void handleSelect(Integer range, AimQuery query, AimSchema schema, AimFilter filter, Queue<String> cmd) throws IOException {
        long t = System.currentTimeMillis();
        t = (System.currentTimeMillis()-t);
        query.range(range);
        Pipe result = query.select(filter, schema.names());
        System.out.println("Filetered & Sorted & Fetch: " + t +" ms");
        pipe.write(true);
        pipe.write("RESULT");
        pipe.write(schema.toString());
        try {
            while(true) {
                AimRecord record = new AimRecord(schema, result, table.sortColumn);
                pipe.write(true);
                pipe.write(record.getBytes());
            }
        } catch (EOFException e) {
            pipe.write(false);
            pipe.flush();
        }

    }

    private AimQuery handleRangeQuery(Queue<String> cmd, AimQuery query) {
        // TODO Auto-generated method stub
        return null;
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

    private AimFilter handleFilterQuery(Integer range, AimQuery query, Queue<String> cmd) throws IOException {
        query.range(range);
        
        String expression = cmd.poll();
        //TODO process cmd
        AimFilter filter = query
                .filter("user_quizzed").in("true");
                //.and("timestamp").greaterThan("20")
                //.and("api_key").contains("test")
                //.and("post_code").not().in("EC2 A1","EC2 A11","EC2 A21");



        long t = System.currentTimeMillis();
        AimFilterSet set = filter.go();
        t = (System.currentTimeMillis()-t);

        pipe.write(true);
        pipe.write("FILTER_SET");
        filter.write(pipe);
        set.write(pipe);

        return filter;

    }

    private Queue<String> parse(String input) throws IOException {
        boolean escape = false;
        boolean string = false;
        boolean whitespace = true;
        String value = "";
        Queue<String> result = new LinkedList<String>();
        Reader in = new StringReader(input);
        while(true) {
            int c;
            if (0 > (c = in.read())) {
                break;
            }
            char ch = (char) c;
            if (string) {
                if (escape) {
                    value += ch; //string escaped char
                    escape = false;
                } else if (ch == '\\') {
                    escape = true;
                } else if (ch =='\'') {
                    result.add(value); string = false; value = ""; 
                } else {
                    value += ch; //string litral char
                }
            } else if (ch == '\'') {
                string = true;
            } else if (ch == ' ') {
                if (!whitespace) {
                    result.add(value); value = "";
                    whitespace = true;
                }
            } else {
                value += ch; //keyword literal char
            }
        }
        result.add(value); 
        return result;
    }

}
