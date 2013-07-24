package net.imagini.aim.node;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;

import net.imagini.aim.AimTable;
import net.imagini.aim.pipes.Pipe;

public class LoaderInterface extends Thread  {

    private Socket socket;
    private AimTable table;

    public LoaderInterface(AimTable table,Socket clientConnection) {
        
        this.socket = clientConnection;
        //TODO hand-shake and init (pipe_type, schema)
        this.table = table;
    }

    @Override public void run() {
        Pipe pipe;
        try {
            pipe = Server.type.getConstructor(InputStream.class).newInstance(socket.getInputStream());
        } catch (IOException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e1) {
            System.out.println("Mock Server failed to establish client connection");
            return;
        }

        try {
            //keep loading
            long t = System.currentTimeMillis();
            try {
                System.out.println("Server Loader activated");
                while (true) {
                    if (interrupted())  break;
                    table.append(pipe);
                }
            } catch (EOFException e) {
                System.out.println("load(EOF) records: " 
                        + table.getCount() 
                        + " time(ms): " + (System.currentTimeMillis() - t)
                );
            } finally {
                table.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                pipe.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("Simple Loader Stopped");
        }
    }
}
