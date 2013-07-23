package net.imagini.aim.console;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.BitSet;
import java.util.UUID;

import net.imagini.aim.AimDataType;
import net.imagini.aim.AimQuery;
import net.imagini.aim.AimTable;
import net.imagini.aim.node.Server;
import net.imagini.aim.pipes.Pipe;
import net.jpountz.lz4.LZ4BlockOutputStream;

public class Console extends Thread {

    private AimTable table;

    public static void main(String[] args) throws IOException {
        Server server = new Server(4000);
        server.start();
        new Console(server.getTestTable()).run();
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
                String[] cols = "timestamp,post_code,userUid,userQuizzed,url".split(",");
                final AimQuery query;

                try {
                    switch(input) {
                        case "": 
                            System.out.println("Table " + table.getName());
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
    
                            BitSet res = query.filter("userQuizzed").in("true")
                                .and("timestamp").smaller("1374598963")
                                .and("api_key").eq("mirror")
                                //.and("region_code").eq("LDN")
                                //.and("post_code").in("EC2 A1","EC2 A11","EC2 A21")
                                .go();
                            t = (System.currentTimeMillis()-t);
    
                            //select
                            result = query.select(res,cols);
                            collectResults(cols, result);
    
                            //debug
                            System.out.println("Filter time(ms): " + t );
                            byte[] bits = res.toByteArray();
                            ByteArrayOutputStream compressed = new ByteArrayOutputStream();
                            LZ4BlockOutputStream lz4 = new LZ4BlockOutputStream(compressed);
                            lz4.write(bits);
                            lz4.close();
                            System.out.println("Filter cardinality/bits/bytes/lz4: " + res.cardinality() + "/" + res.length() + "/" + bits.length + "/" + compressed.size());
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
                    AimDataType type = table.def(col);
                    byte[] value = result.read(type);
                    String textValue;
                    switch(col) {
                        case "userUid":
                            textValue = new UUID(Pipe.readLong(value,0) , Pipe.readLong(value,8)).toString();
                        break;
                        default:
                            textValue = Pipe.convert(type, value);
                        break;
                    }
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
