package net.imagini.aim.console;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;

import joptsimple.internal.Strings;
import net.imagini.aim.Aim.SortOrder;
import net.imagini.aim.AimSchema;
import net.imagini.aim.AimUtils;
import net.imagini.aim.loaders.EventsSchema;
import net.imagini.aim.loaders.TestEventsLoader;
import net.imagini.aim.node.AimTable;
import net.imagini.aim.node.TableServer;
import net.imagini.aim.pipes.Pipe;
import net.imagini.aim.pipes.Pipe.Protocol;
import net.imagini.aim.pipes.PipeLZ4;

public class Console extends Thread {

    final private Pipe pipe;
    
    static private TableServer server;
    static private Thread loader;

    public static void main(String[] args) throws IOException {
        System.out.println("\nAIM/CASSPAR Test Console\n");

        AimTable table = new AimTable("events", 4194304, new EventsSchema(), "user_uid", SortOrder.DESC);
        server = new TableServer(table, 4000);
        server.start();

        loader = new TestEventsLoader(table);
        /**
        loader = new CSVLoader(new String[]{
                "--gzip", 
                "--limit","1000000",
                "--schema","timestamp(LONG),client_ip(IPV4:INT),event_type(STRING),user_agent(STRING),country_code(BYTEARRAY[2]),region_code(BYTEARRAY[3]),post_code(STRING),api_key(STRING),url(STRING),user_uid(UUID:BYTEARRAY[16]),user_quizzed(BOOL)",
                "/Users/mharis/events-2013-07-23.csv.gz"
        });
        /**/
        loader.start();

        new Console("localhost", 4000).run();

    }

    private Socket socket;

    public Console(String host, int port) throws IOException {
        socket = new Socket(InetAddress.getByName(host), port);
        pipe = new PipeLZ4(socket, Protocol.QUERY);
        print("<Enter> for STATS about the table");
        print("FILTER <field> (=|>|<|IN|CONTAINS) '<value>' [(and|or|not) ...) - to setup a filter and see cardinality of it");
        print("SELECT [<field>[,<field>[,..]] - to select the records that match previously set filter");
        print("EXIT to exit");
    }

    private void print(String data) {
        System.out.println(data);
    }

    @Override public void run() {
        try {
            BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
            while(true) {
                System.out.println();
                System.out.print(">");
                String instruction = bufferRead.readLine().trim();
                String[] input = instruction.split("\\s+",1);
                try {
                    switch(input[0]) {
                        case "exit": return;
                        case "": 
                            pipe.write("STATS").flush();
                            processResponse();
                            continue;
                        default: 
                            long t = (System.currentTimeMillis());
                            pipe.write(instruction).flush();
                            processResponse();
                            print("Query took: " + (System.currentTimeMillis() - t) + " ms");
                            break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e1) {} finally {
            System.out.println("Console shutting down..");
            try { pipe.close();  } catch (IOException e2) {} 
            if (loader != null) loader.interrupt();
            server.interrupt();
        }
    }

    private void processResponse() throws IOException {
        while(pipe.readBool()) {
            String filter;
            Long count;
            switch(pipe.read()) {
                case "STATS":
                    System.out.println("Table name: " + pipe.read()); 
                    System.out.println("Schema: " + pipe.read());
                    System.out.println("Total records: " + pipe.readLong()); 
                    System.out.println("Total segments: " + pipe.readInt());
                    long size = pipe.readLong();
                    long originalSize = pipe.readLong();
                    System.out.print("Total compressed/original size: " 
                            + (size / 1024 / 1024) + " Mb / "
                            + (originalSize / 1024 / 1024));
                    if (originalSize>0) System.out.print(" Mb = " + ( size * 100 / originalSize ) + "%");
                    System.out.println();
                    break;
                case "COUNT":
                    filter = pipe.read();
                    count = pipe.readLong();
                    long totalCount = pipe.readLong();
                    print("Filter: " + filter);
                    print("Count: " + count + "/" + totalCount);
                    break;
                case "RESULT":
                    AimSchema schema = AimUtils.parseSchema(pipe.read());
                    filter = pipe.read();
                    //long count = 0;
                    while (pipe.readBool()) {
                        print(Strings.join(AimUtils.collect(schema, pipe), ", "));
                        //count++;
                    }
                    long filteredCount = pipe.readLong();
                    count = pipe.readLong();
                    String error = pipe.read();
                    print("Schema: " + schema);
                    print("Filter: " + filter);
                    print((Strings.isNullOrEmpty(error) ? "OK" : "SERVER-SIDE EXCEPTION: " + error) +" Num.records: " + filteredCount + "/" + count);
                    break;
                case "ERROR":
                   print("Error: " + pipe.read());
                   break;
                default:
                    throw new IOException("Stream is curroupt, clsing..");
            }
        }
    }

}
