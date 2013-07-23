package net.imagini.aim.node;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedHashMap;

import net.imagini.aim.Aim;
import net.imagini.aim.AimDataType;
import net.imagini.aim.AimTable;
import net.imagini.aim.pipes.Pipe;
import net.imagini.aim.pipes.PipeLZ4;


public class Server extends Thread {
    ServerSocket loaderInterfaceSocket;

    public static Class<? extends Pipe> type = PipeLZ4.class;

    @SuppressWarnings("serial")
    private static AimTable events = new AimTable("events", 50000, new LinkedHashMap<String,AimDataType>() {{ 
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

    private Thread loaderAcceptor;
    private Thread loader;

    public Server(int port) throws IOException {
        //TODO Query Interface

        //Loader Interface
        loaderInterfaceSocket = new ServerSocket(port);
        loaderAcceptor = new Thread() {
            @Override public void run() {
                System.out.println("Mock Server Acceptor started");
                try {
                    while(true) {
                        Socket socket = loaderInterfaceSocket.accept();
                        //TODO establish which table is this loader for and keep accepting
                        loader = new LoaderInterface(events,socket);
                        loader.start();
                    }
                } catch (IOException e) {
                    System.out.println("Mock Server failed to establish accepted client connection");
                }
            }
        };
    }

    public AimTable getTestTable() {
        return events;
    }

    @Override public void run() {
        loaderAcceptor.start();
        try {
            loaderAcceptor.join();
        } catch (InterruptedException e1) {
            loaderAcceptor.interrupt();
        } finally {
            try {
                loaderInterfaceSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (loader != null) {
                loader.interrupt();
            }
            System.err.println("Mock AIM Server Closed");
        }
    }
}
