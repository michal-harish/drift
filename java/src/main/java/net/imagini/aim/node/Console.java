package net.imagini.aim.node;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;

import net.imagini.aim.AimFilterSet;
import net.imagini.aim.AimQuery;
import net.imagini.aim.AimType;
import net.imagini.aim.pipes.Pipe;
import net.imagini.aim.pipes.PipeLZ4;

import org.apache.commons.io.output.ByteArrayOutputStream;

public class Console extends Thread {

    private AimTable table;

    public static void main(String[] args) throws IOException {
        new Console(
            new AimTable("events", 100000, new EventsSchema())
        ).run();
    }

    public Console(AimTable table) {
        this.table = table;
    }

    @Override public void run() {
        try {
            BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
            while(true) {
                System.out.println();
                System.out.print(">");
                String input = bufferRead.readLine();
                Pipe result = null;
                String[] cols = "timestamp,post_code,user_uid,user_quizzed,url".split(",");
                final AimQuery query;

                try {
                    switch(input) {
                        case "": 
                            System.out.println("Table " + table.name);
                            System.out.println("Total records: " + table.getCount());
                            System.out.println("Total segments: " + table.getNumSegments());
                            System.out.print("Total compressed/original size: " 
                                    + (table.getSize() / 1024 / 1024) + " Mb / "
                                    + (table.getOriginalSize() / 1024 / 1024));
                            if (table.getOriginalSize()>0) System.out.print(" Mb = " + ( table.getSize() * 100 / table.getOriginalSize() ) + "%");
                            System.out.println();
                            continue;
                        case "exit": throw new InterruptedException();
                        case "test": 
                            query = new AimQuery(table);
                            //filter
                            long t = System.currentTimeMillis();
    
                            AimFilterSet set = query.filter("user_quizzed").in("true")
                                .and("timestamp").greaterThan("20")
                                .and("api_key").contains("test")
                                .and("post_code").not().in("EC2 A1","EC2 A11","EC2 A21")
                                .go();
                            t = (System.currentTimeMillis()-t);

                            //select
                            //result = query.select(set,cols);
                            //collectResults(cols, result);

                            //debug
                            System.out.println("Filter time(ms): " + t );

                            ByteArrayOutputStream compressed = new ByteArrayOutputStream();
                            Pipe test = new PipeLZ4(compressed); set.write(test); test.close();
                            System.out.println("Filter cardinality/bits/lz4size: " + set.cardinality() + "/" + set.length() + "/" + compressed.size());
                            System.out.println();

                            break;
                        case "last": //default query
                            try {
                                query = new AimQuery(table, table.getNumSegments()-1);
                                result = query.select(cols);
                                collectResults(cols, result);
                            } catch (Exception e) {
                                e.printStackTrace();
                                continue;
                            }
                            break;
                        default: //custom select query
                            cols = input.split(",");
                            try {
                                query = new AimQuery(table, table.getNumSegments()-1);
                                result = query.select(cols);
                                collectResults(cols, result);
                            } catch (Exception e) {
                                e.printStackTrace();
                                continue;
                            }
                        break;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch(InterruptedException e) {
            //
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    private void collectResults(final String[] cols, final Pipe result) throws IOException {
        if (result == null) return;
        int count = 0;
        long t = System.currentTimeMillis();
        try {
            while(true) {
                for(String col: cols) {
                    AimType type = table.def(col); 
                    byte[] value = result.read(type.getDataType());
                    String textValue = type.convert(value);
                    System.out.print(textValue + " ");
                }
                count++;
                System.out.println();
            }
        } catch (EOFException e) {
            System.out.println("\n(EOF) Selected records: " + count + ", retreive time(ms): " + (System.currentTimeMillis() - t));
        } 
    }
}
