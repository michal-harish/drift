package net.imagini.aim.console;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;

import joptsimple.internal.Strings;
import net.imagini.aim.AimFilterSet;
import net.imagini.aim.AimSchema;
import net.imagini.aim.AimUtils;
import net.imagini.aim.loaders.TestEventsLoader;
import net.imagini.aim.node.AimTable;
import net.imagini.aim.node.EventsSchema;
import net.imagini.aim.node.TableServer;
import net.imagini.aim.pipes.Pipe;
import net.imagini.aim.pipes.Pipe.Protocol;
import net.imagini.aim.pipes.PipeLZ4;

public class Console extends Thread {

    final private Pipe pipe;

    public static void main(String[] args) throws IOException {
        AimTable table = new AimTable("events", 100000, new EventsSchema());
        new TableServer(table, 4000).start();
        new TestEventsLoader().start();
        new Console("localhost", 4000).run();
    }

    private Socket socket;

    public Console(String host, int port) throws IOException {
        socket = new Socket(InetAddress.getByName(host), port);
        pipe = new PipeLZ4(socket, Protocol.QUERY);
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
                String[] input = bufferRead.readLine().trim().split("\\s+",1);
                try {
                    switch(input[0]) {
                        case "exit": return;
                        case "": 
                            pipe.write("STATS").flush();
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
                            continue;
                        //TODO case "range": break;
                        case "filter":
                            long t = (System.currentTimeMillis());
                            pipe.write("FILTER").flush();
                            AimFilterSet set = new AimFilterSet(pipe);
                            System.out.println("Filter time(ms): " + (System.currentTimeMillis()-t) );
                            System.out.println("Filter cardinality/bits/lz4size: " + set.cardinality() + "/" + set.length() );
                            System.out.println();
                            break;
                        //TODO case "select": cols = input[1].split(",");break;
                        case "last":
                            pipe.write("LAST").flush();
                            AimSchema schema = AimUtils.parseSchema(pipe.read());
                            long count = 0;
                            while (pipe.readBool()) {
                                print(Strings.join(AimUtils.collect(schema, pipe), ", "));
                                count++;
                            }
                            print("Num.records: " + count);
                            break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e1) {
            e1.printStackTrace();
        } finally {
            try {
                pipe.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}
