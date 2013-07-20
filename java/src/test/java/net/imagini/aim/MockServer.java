package net.imagini.aim;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.net.Socket;

public class MockServer extends Thread {
    public static Class<? extends Pipe> type = Pipe.class;
    ServerSocket serverSocket;

    public MockServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);

    }

    public void run() {
        try {
           Socket socket = serverSocket.accept();
           Pipe pipe = type.getConstructor(InputStream.class).newInstance(socket.getInputStream());
           int count = 0;
           long t = System.currentTimeMillis();
           try {
               while (true) {
                   Event event = new Event(pipe);
                   count ++;
                   String eventTrace = event.toString();
                   System.out.println(count + " " + eventTrace);
               }
           } catch (EOFException e) {
               System.out.println("\nEOF events: " + count + " time(ms): " + (System.currentTimeMillis() - t));
           }
           pipe.close();
           serverSocket.close();
           System.out.println("Mock AIM Server Closed");
        } catch (IOException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
