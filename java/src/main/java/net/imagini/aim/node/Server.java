package net.imagini.aim.node;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import net.imagini.aim.AimTable;
import net.imagini.aim.EventsSchema;
import net.imagini.aim.pipes.Pipe;
import net.imagini.aim.pipes.PipeLZ4;


public class Server extends Thread {
    ServerSocket loaderInterfaceSocket;

    public static Class<? extends Pipe> type = PipeLZ4.class;

    //TODO Node Server should not be aware about tables
    private static AimTable events = new AimTable("events", 1000000, new EventsSchema());

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
