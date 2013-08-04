package net.imagini.aim.loaders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

import net.imagini.aim.Aim;
import net.imagini.aim.node.AimTable;
import net.imagini.aim.node.Segment;

public class TestEventsLoader extends Thread{

    private AimTable table;
    private Segment currentSegment; 
    private ByteBuffer buffer = ByteBuffer.allocate(1000000);

    public TestEventsLoader(AimTable table) {
        this.table = table;
    }

    public void run() {
        try {
            buffer.order(ByteOrder.BIG_ENDIAN);
            for (long i = 1; i <=10000000; i++) {
                if (currentSegment == null) {
                    currentSegment = new Segment(table.schema);
                }
                try {
                    UUID userUid  = UUID.randomUUID();
                    buffer.putLong(i); 
                    buffer.put(Aim.IPV4(Aim.INT).convert("173.194.41.99"));
                    buffer.put(Aim.STRING.convert("VDNAUserTestEvent"));
                    buffer.put(Aim.STRING.convert("user agent info .."));
                    buffer.put(Aim.BYTEARRAY(2).convert("GB"));
                    buffer.put(Aim.BYTEARRAY(3).convert("LDN"));
                    buffer.put(Aim.STRING.convert("EC2 A"+ i));
                    buffer.put(Aim.STRING.convert("test"));
                    buffer.put(Aim.STRING.convert("http://"));
                    buffer.put(Aim.BYTEARRAY(16).convert(userUid.toString()));
                    buffer.put(Aim.BOOL.convert(userUid.hashCode() % 100 == 0 ? "true" : "false"));
                    commitCurrentSegment();
                } catch(Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
            commitCurrentSegment();
        } catch (Exception  ex) {
           ex.printStackTrace();
           System.exit(0);
        } 
    }

    private void commitCurrentSegment() throws IOException {
        if (currentSegment != null) {
            boolean commit = currentSegment.getSize() > 2000000L;
            if (commit || buffer.position() > 65535) {
                buffer.flip();
                currentSegment.append(buffer);
                buffer.clear();
            }
            if (commit) {
                commitCurrentSegment();
                try {
                    currentSegment.close();
                    if (currentSegment.getOriginalSize() > 0) {
                        table.add(currentSegment);
                    }
                    currentSegment = null;
                } catch (IllegalAccessException e) {
                    throw new IOException(e);
                }
            }
        }
    }
}
