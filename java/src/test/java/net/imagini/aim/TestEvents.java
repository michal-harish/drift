package net.imagini.aim;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.UUID;

public class TestEvents {
    public static void main(String[] args) throws InterruptedException, IOException {

        MockServer server = new MockServer(4000);
        server.start();

        try {
            Socket socket = new Socket(
                InetAddress.getByName("localhost"), //10.100.11.239 
                4000
            );
            final Pipe pipe = MockServer.type.getConstructor(OutputStream.class).newInstance(socket.getOutputStream());

            try {
                for (int i = 1; i <=100000; i++) {
                    UUID userUid  = UUID.randomUUID();
                    InetAddress clientIp = InetAddress.getByName("173.194.41.99");
                    pipe.write(10000L + i);
                    pipe.write(Arrays.copyOfRange(clientIp.getAddress(),0,4));
                    pipe.write("VDNAUserTestEvent");
                    pipe.write("user agent info ..");
                    pipe.write(Arrays.copyOfRange("GB".getBytes(), 0, 2));
                    pipe.write(Arrays.copyOfRange("LDN".getBytes(), 0, 3));
                    pipe.write("EC2 A"+ i);
                    pipe.write("test");
                    pipe.write("http://");
                    pipe.write(userUid.getMostSignificantBits());pipe.write(userUid.getLeastSignificantBits());
                    pipe.write(false);
                    Thread.sleep(10);
                }
            } catch(Exception e) {
               e.printStackTrace();
            }
            pipe.close();
            socket.close();
        }
        catch (IOException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException ex) {
           ex.printStackTrace();
        }

        server.interrupt();

    }
}
