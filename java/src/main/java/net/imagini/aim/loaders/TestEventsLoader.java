package net.imagini.aim.loaders;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.UUID;

import net.imagini.aim.AimSchema;
import net.imagini.aim.pipes.Pipe;
import net.imagini.aim.pipes.PipeLZ4;

public class TestEventsLoader extends Thread{

    public static void main(String[] args) throws InterruptedException, IOException {
        TestEventsLoader loader = new TestEventsLoader();
        loader.start();
        loader.join();
    }

    public void run() {
        try {
            Socket socket = new Socket(
                InetAddress.getByName("localhost"), //10.100.11.239 
                4000
            );
            final Pipe pipe = new PipeLZ4(socket.getOutputStream(), Pipe.Protocol.LOADER);
            AimSchema schema = new EventsSchema();
            pipe.write(schema.toString());
            for (long i = 1; i <=3000; i++) {
                try {
                    UUID userUid  = UUID.randomUUID();
                    pipe.write(i);
                    pipe.write(schema.def("client_ip").convert("173.194.41.99"));
                    pipe.write("VDNAUserTestEvent");
                    pipe.write("user agent info ..");
                    pipe.write(Arrays.copyOfRange("GB".getBytes(), 0, 2));
                    pipe.write(Arrays.copyOfRange("LDN".getBytes(), 0, 3));
                    pipe.write("EC2 A"+ i);
                    pipe.write("test");
                    pipe.write("http://");
                    pipe.write(schema.def("user_uid").convert(userUid.toString()));
                    pipe.write(userUid.hashCode() % 100 == 0);
                } catch(IOException e) {
                    e.printStackTrace();
                    break;
                }
            }
            pipe.close();
            socket.close();
        } catch (Exception  ex) {
           ex.printStackTrace();
           System.exit(0);
        } 
    }
}
