package net.imagini.aim;

import java.io.IOException;
import java.net.InetAddress;

public class AimPrototype {

    public static void main(String[] args) throws InterruptedException, IOException {

        //MockServer server = new MockServer(4000);
        //server.start();

        try {
            final AimPipe pipe = new AimPipe(InetAddress.getByName("10.100.11.239"), 4000);
            //pipe.write("Hello there " + UUID.randomUUID().toString());
            //pipe.write("Hello there " + UUID.randomUUID());
            //pipe.write("Hello there " + UUID.randomUUID());
            Event e = new Event(
                System.currentTimeMillis()/1000, 
                InetAddress.getByName("google.com"), 
                "VDNAUserPageview", 
                "userAgent", 
                "GB", "LDN", "EC2 123", "test", 
                "http://blah.com", 
                "81482d5b-64c9-4c29-81e0-913878f8c91e", 
                false
            );
            e.write(pipe);
            pipe.close();
        }
        catch (IOException ex) {
           ex.printStackTrace();
        }

        //Thread.sleep(1000);
        //server.interrupt();


    }
}
