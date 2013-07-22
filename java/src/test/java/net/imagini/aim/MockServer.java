package net.imagini.aim;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import net.jpountz.lz4.LZ4BlockOutputStream;

import org.apache.commons.io.EndianUtils;


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
                Pipe[] result = null;
                String[] cols = "timestamp,post_code,userUid,userQuizzed,url".split(",");
                final AimQuery query;
                switch(input) {
                    case "exit": throw new InterruptedException();
                    case "test": 
                        query = new AimQuery(MockServer.events);
                        //filter
                        long t = System.currentTimeMillis();
                        ExecutorService collector = Executors.newFixedThreadPool(3);
                        Future<BitSet> set1 = collector.submit(new Callable<BitSet>() {
                            @Override public BitSet call() throws Exception { return query.filter("api_key").any("test"); }
                        });
                        Future<BitSet> set2 = collector.submit(new Callable<BitSet>() {
                            @Override public BitSet call() throws Exception { return query.filter("post_code").any("EC2 A1","EC2 A8","EC2 A1000000","EC2 A6987000","EC2 A6987005"); }
                        });
                        Future<BitSet> set3 = collector.submit(new Callable<BitSet>() {
                            @Override public BitSet call() throws Exception { return query.filter("userQuizzed").any("true","false"); }
                        });
                        BitSet res = set1.get();
                        res.and( set2.get() );
                        res.and( set3.get() );
                        collector.shutdownNow();
                        System.out.println("Filter time(ms): " + (System.currentTimeMillis()-t) );
                        byte[] bits = res.toByteArray();
                        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
                        LZ4BlockOutputStream lz4 = new LZ4BlockOutputStream(compressed);
                        lz4.write(bits);
                        lz4.close();
                        System.out.println("Filter cardinality/bits/bytes/lz4: " + res.cardinality() + "/" + res.length() + "/" + bits.length + "/" + compressed.size());
                        System.out.println();
 
                        //select
                        result = query.select(res,cols);
                        collectResults(cols, result);

                        break;
                    case "": //default query
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
                System.out.println("Total segments: " + events.getNumSegments());
                System.out.println("Total records: " + events.getCount());
            }
        } catch(InterruptedException e) {
            acceptor.interrupt();
            loader.interrupt();
            //
        } catch(IOException | ExecutionException e) {
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

    private void collectResults(final String[] cols, final Pipe[] result) throws IOException {
        if (result == null) return;
        int count = 0;
        long t = System.currentTimeMillis();
        try {
            ExecutorService collector = Executors.newFixedThreadPool(cols.length);
            @SuppressWarnings("unchecked")
            Future<String>[] row = new Future[cols.length];
            new ArrayList<Future<String>>(cols.length).toArray(row);
            while(true) {
                int colIndex = 0; 
                for(final String col: cols) {
                    final int j = colIndex++;
                    row[j] = collector.submit(new Callable<String>() {
                        @Override
                        public String call() throws IOException {
                            AimDataType type = events.column(col).type;
                            byte[] value = result[j].read(type);
                            switch(col) {
                                default: return Aim.convert(type, value);
                                case "userUid":
                                    InputStream bi = new ByteArrayInputStream(value);
                                    return new UUID(EndianUtils.readSwappedLong(bi) , EndianUtils.readSwappedLong(bi)).toString();
                            }
                        }
                    });
                }
                for(Future<String> textValue: row) {
                    System.out.print(textValue.get() + " ");
                }
                
                /*
                j = 0; 
                for(String col: cols) {
                    Pipe p = result[j++];
                    AimDataType type = events.column(col).type;
                    byte[] value = p.read(type);
                    String textValue;
                    switch(col) {
                        case "userUid":
                            InputStream bi = new ByteArrayInputStream(value);
                            textValue = new UUID(EndianUtils.readSwappedLong(bi) , EndianUtils.readSwappedLong(bi)).toString();
                        break;
                        default:
                            textValue = Aim.convert(type, value);
                        break;
                    }
                    System.out.print(textValue + " ");
                }
                */
                count++;
                System.out.println();
            }
        } /*catch (EOFException e) {
            System.out.println("\n(EOF) Selected records: " + count + ", retreive time(ms): " + (System.currentTimeMillis() - t));
        } */catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof EOFException) {
                System.out.println("\n(EOF) Selected records: " + count + ", retreive time(ms): " + (System.currentTimeMillis() - t));
            } else {
                e.printStackTrace();
            }
        }
    }
}
