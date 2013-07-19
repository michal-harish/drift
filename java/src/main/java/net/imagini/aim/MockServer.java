package net.imagini.aim;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class MockServer extends Thread {
    ServerSocket serverSocket;
    private DataInputStream in; 
    public MockServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);

    }

    public void run() {
        try {
           Socket socket = serverSocket.accept();
           in = new DataInputStream(socket.getInputStream());
           System.out.println("Mock AIM Server Started");
           String field = null;
           while (true) {
              if (in.available()>0) {
                  field = AimPipe.readString(in);
                  System.out.println("Read:" + field);
              } else {
                  try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                   break;
                }
              }
           }
           in.close();
           serverSocket.close();
           System.out.println("Mock AIM Server Closed");
        } catch (IOException ex) {
           ex.printStackTrace();
        }
    }
}
