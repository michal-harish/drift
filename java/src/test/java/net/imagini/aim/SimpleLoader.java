package net.imagini.aim;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;

public class SimpleLoader extends Thread {

    private Socket socket;

    public SimpleLoader(Socket clientConnection) {
        this.socket = clientConnection;
    }

    @Override public void run() {
        Pipe pipe;
        try {
            pipe = MockServer.type.getConstructor(InputStream.class).newInstance(socket.getInputStream());
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
                    MockServer.events.append(pipe);
                }
            } catch (EOFException e) {
                System.out.println("load(EOF) records: " 
                        + MockServer.events.getCount() 
                        + " time(ms): " + (System.currentTimeMillis() - t)
                );
            } finally {
                MockServer.events.closeSegment();
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
