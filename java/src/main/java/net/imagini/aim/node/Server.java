package net.imagini.aim.node;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import net.imagini.aim.AimTable;
import net.imagini.aim.LoaderInterface;
import net.imagini.aim.console.EventsSchema;
import net.imagini.aim.pipes.Pipe;
import net.imagini.aim.pipes.PipeLZ4;


public class Server extends Thread {
    ServerSocket controllerListener;

    public static Class<? extends Pipe> type = PipeLZ4.class;

    private Thread controllerAcceptor;
    private Thread loader;

    public Server(int port) throws IOException {
        controllerListener = new ServerSocket(port);
        controllerAcceptor = new Thread() {
            @Override public void run() {
                System.err.println("Casspar Node Server Started");
                try {
                    while(true) {
                        new Thread(
                            new ClientConnection(controllerListener.accept())
                        ).start();
                    }
                } catch (IOException e) {
                    System.out.println("Mock Server failed to establish accepted client connection");
                }
            }
        };
    }

    @Override public void run() {
        controllerAcceptor.start();
        try {
            controllerAcceptor.join();
        } catch (InterruptedException e1) {
            controllerAcceptor.interrupt();
        } finally {
            try {
                controllerListener.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (loader != null) {
                loader.interrupt();
            }
            System.err.println("Casspar Node Server Closed");
        }
    }
}
