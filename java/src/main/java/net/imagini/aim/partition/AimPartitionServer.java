package net.imagini.aim.partition;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import net.imagini.aim.Pipe;


public class AimPartitionServer extends Thread {
    ServerSocket controllerListener;
    private Thread controllerAcceptor;
    private AimPartition partition;
    public final int port;

    public AimPartitionServer(AimPartition partition, int port) throws IOException {
        this.partition = partition;
        this.port = port;

        controllerListener = new ServerSocket(port);
        controllerAcceptor = new Thread() {
            @Override public void run() {
                System.out.println("Aim Partition Server accepting connections on port " + AimPartitionServer.this.port + " " + AimPartitionServer.this.partition.schema);
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
                                case LOADER: new AimPartitionServerLoaderSession(AimPartitionServer.this.partition,pipe).start(); break;
                                case QUERY: new AimPartitionServerQuerySession(AimPartitionServer.this.partition,pipe).start(); break;
                                default: System.out.println("Unsupported protocol request " + pipe.protocol);
                            }
                        } catch (IOException e) {
                            System.out.println("Aim Server at port "+AimPartitionServer.this.port +" failed to establish client connection: " + e.getMessage());
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
            System.out.println("Aim Server at port "+AimPartitionServer.this.port + " shut down.");
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
