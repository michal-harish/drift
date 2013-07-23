package net.imagini.aim;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.UUID;

import net.jpountz.lz4.LZ4BlockOutputStream;


public class MockServer extends Thread {
    ServerSocket serverSocket;

    public static Class<? extends Pipe> type = PipeLZ4.class;

    @SuppressWarnings("serial")
    public static AimTable events = new AimTable("events", 10000, new LinkedHashMap<String,AimDataType>() {{ 
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
                Pipe result = null;
                String[] cols = "timestamp,post_code,userUid,userQuizzed,url".split(",");
                final AimQuery query;

                switch(input) {
                    case "": 
                        System.out.println("Total records: " + events.getCount());
                        System.out.println("Total segments: " + events.getNumSegments());
                        System.out.print("Total compressed/original size: " 
                                + (events.getSize() / 1024 / 1024) + " Mb / "
                                + (events.getOriginalSize() / 1024 / 1024));
                        if (events.getOriginalSize()>0) System.out.print(" Mb = " + ( events.getSize() * 100 / events.getOriginalSize() ) + "%");
                        System.out.println();
                        continue;
                    case "exit": throw new InterruptedException();
                    case "test": 
                        query = new AimQuery(MockServer.events);
                        
                        //filter
                        long t = System.currentTimeMillis();

                        BitSet res = query.filter("userQuizzed").in("true")
                            .and("timestamp").greater("1374598963")
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
                            query = new AimQuery(MockServer.events, MockServer.events.getNumSegments()-1);
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
                            query = new AimQuery(MockServer.events, MockServer.events.getNumSegments()-1);
                            result = query.select(cols);
                            collectResults(cols, result);
                        } catch (Exception e) {
                            e.printStackTrace();
                            continue;
                        }
                    break;
                }
            }
        } catch(InterruptedException e) {
            acceptor.interrupt();
            loader.interrupt();
            //
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
            System.err.println("Mock AIM Server Closed");
        }
    }

    private void collectResults(final String[] cols, final Pipe result) throws IOException {
        if (result == null) return;
        int count = 0;
        long t = System.currentTimeMillis();
        try {
            while(true) {
                for(String col: cols) {
                    AimDataType type = events.def(col);
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
