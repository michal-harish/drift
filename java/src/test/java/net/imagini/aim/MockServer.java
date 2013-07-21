package net.imagini.aim;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.UUID;

public class MockServer extends Thread {
    ServerSocket serverSocket;

    public static Class<? extends Pipe> type = PipeLZ4.class;

    @SuppressWarnings("serial")
    public static AimTable events = new AimTable("events", 1000, new LinkedHashMap<String,AimDataType>() {{ 
        put("timestamp", Aim.LONG);
        put("client_ip", Aim.INT);
        put("type", Aim.STRING);
        put("user_agent", Aim.STRING);
        put("country_code", Aim.BYTEARRAY(2));
        put("region_code", Aim.BYTEARRAY(3));
        put("post_code", Aim.STRING);
        put("api_key", Aim.STRING);
        put("url", Aim.STRING);
        put("userUid", Aim.BYTEARRAY(16));
        put("userQuizzed", Aim.BOOL);
    }});

    private Thread acceptor;
    private Thread loader;

    public MockServer(int port) throws IOException {
        //Loader Listener 
        serverSocket = new ServerSocket(port);
        //Loader Acceptor 
        acceptor = new Thread() {
            @Override public void run() {
                System.out.println("Mock Server Acceptor started");
                try {
                    Socket socket = serverSocket.accept();
                    loader = new SimpleLoader(socket);
                    loader.start();
                } catch (IOException e) {
                    System.out.println("Mock Server failed to establish accepted client connection");
                }
            }
        };
    }

    @Override public void run() {
        acceptor.start();
        try {
            BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
            while(true) {
                System.out.println();
                System.out.print(">");
                String input = bufferRead.readLine();
                switch(input) {
                    case "test": 
                        BitSet res = new AimFilter(events.columns.get("post_code")).any("EC2 A1","EC2 A8","EC2 A1000000");
                        res.and(new AimFilter(events.columns.get("api_key")).any("test"));
                        System.out.println("(post_code='EC2 A1' || post_code='EC2 A8' || post_code='EC2 A1000000') && api_key='test' cardinality: " + res.cardinality());
                        break;
                    case "": input = "timestamp,post_code,userUid";
                    default:
                        if (events.getNumSegments() > 0) {
                            //query
                            int count = 0;
                            long t = System.currentTimeMillis();
                            try {
                                String[] cols = input.split(",");
                                Pipe[] result;
                                try {
                                    result = events.select(cols);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    continue;
                                }
                                while(true) {
                                    int i = 0; for(String col: cols) {
                                        Pipe p = result[i++];
                                        String textValue;
                                        switch(col) {
                                            case "userUid": textValue = new UUID(p .readLong(),p .readLong()).toString();
                                            break;
                                            default: textValue = events.readAsText(col,p);
                                            break;
                                        }
                                        System.out.print(textValue + " ");
                                    }
                                    System.out.println();
                                }
                            } catch (EOFException e) {
                                System.out.println("\nselect(EOF) results: " + count + " time(ms): " + (System.currentTimeMillis() - t));
                            }

                            System.out.println("Num.segments: " + events.getNumSegments());
                        } else {
                            System.out.println("No closed segments yet");
                        }
                        break;
                }
                System.out.println("Num.records: " + events.getCount());
            }
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            try {
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (loader != null) {
                loader.interrupt();
            }
            System.out.println("Mock AIM Server Closed");
        }
    }
}
