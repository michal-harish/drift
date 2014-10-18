package net.imagini.aim.cluster;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import net.imagini.aim.AimPartition;
import net.imagini.aim.Pipe;


public class AimServer extends Thread {
    ServerSocket controllerListener;
    private Thread controllerAcceptor;
    private AimPartition table;
    public final int port;

    public AimServer(AimPartition table, int port) throws IOException {
        this.table = table;
        this.port = port;

        controllerListener = new ServerSocket(port);
        controllerAcceptor = new Thread() {
            @Override public void run() {
                System.out.println("Aim Server accepting connections on port " + AimServer.this.port + " " + AimServer.this.table.schema);
                    while(true) {
                        Socket socket;
                        try {
                            socket = controllerListener.accept();
                            if (interrupted()) break;
                        } catch (IOException e) {
                            System.out.println(e.getMessage());
                            break;
                        }
                        Pipe pipe;
                        try {
                            if (interrupted()) break;
                            pipe = Pipe.open(socket);
                            System.out.println(pipe.protocol + " connection from " + socket.getRemoteSocketAddress().toString());
                            switch(pipe.protocol) {
                                //TODO case BINARY: new ReaderThread(); break;
                                case LOADER: new TableServerLoaderSession(AimServer.this.table,pipe).start(); break;
                                case QUERY: new TableServerQuerySession(AimServer.this.table,pipe).start(); break;
                                default: System.out.println("Unsupported protocol request " + pipe.protocol);
                            }
                        } catch (IOException e) {
                            System.out.println("Aim Server at port "+AimServer.this.port +" failed to establish client connection: " + e.getMessage());
                            continue;
                        }
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
            close();
            System.out.println("Aim Server at port "+AimServer.this.port + " shut down.");
        }
    }

    public void close() {
        try {
            controllerListener.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //TODO close all connections
    }
}
