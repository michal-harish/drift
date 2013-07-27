package net.imagini.aim.node;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import net.imagini.aim.pipes.Pipe;


public class TableServer extends Thread {
    ServerSocket controllerListener;
    private Thread controllerAcceptor;
    private AimTable table;

    public TableServer(AimTable table, int port) throws IOException {
        this.table = table;

        controllerListener = new ServerSocket(port);
        controllerAcceptor = new Thread() {
            @Override public void run() {
                System.out.println("Table Server("+TableServer.this.table.name+") accepting connections");
                    while(true) {
                        Socket socket;
                        try {
                            socket = controllerListener.accept();
                            if (interrupted()) break;
                        } catch (IOException e) {
                            e.printStackTrace();
                            break;
                        }
                        Pipe pipe;
                        try {
                            if (interrupted()) break;
                            pipe = Pipe.open(socket);
                            System.out.println(pipe.protocol + " connection from " + socket.getRemoteSocketAddress().toString());
                            switch(pipe.protocol) {
                                //TODO case BINARY: new ReaderThread(); break;
                                case LOADER: new TableServerLoaderThread(TableServer.this.table,pipe).start(); break;
                                case QUERY: new TableServerQueryThread(TableServer.this.table,pipe).start(); break;
                                default: System.out.println("Unsupported protocol request " + pipe.protocol);
                            }
                        } catch (IOException e) {
                            System.out.println("Table Server("+TableServer.this.table.name+") failed to establish client connection: " + e.getMessage());
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
            System.err.println("Table Server("+TableServer.this.table.name+") closed.");
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
