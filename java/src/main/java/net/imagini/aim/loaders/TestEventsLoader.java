package net.imagini.aim.loaders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import net.imagini.aim.Aim;
import net.imagini.aim.AimSegment;
import net.imagini.aim.node.Segment;
import net.imagini.aim.node.SegmentSorted;
import net.imagini.aim.table.AimTable;

public class TestEventsLoader extends Thread{

    private AimTable table;
    private AimSegment currentSegment; 
    private ByteBuffer record = ByteBuffer.allocate(1000000);
    private long limit;

    public TestEventsLoader(AimTable table, long limit) {
        this.table = table;
        this.limit = limit;
    }

    public void run() {
        try {
            record = ByteBuffer.allocate(65535);
            record.order(Aim.endian);
//            InputStream stream = new ByteBufferInputStream(record);

            for (long i = 1; i <= limit; i++) {
                createNewSegmentIfNull();
                try {
                    record.clear();

                    UUID userUid  = UUID.nameUUIDFromBytes(String.valueOf(i+1000000).getBytes());
                    record.putLong(i); 
                    record.put(Aim.IPV4(Aim.INT).convert("173.194.41.99"));
                    record.put(Aim.STRING.convert("VDNAUserTestEvent"));
                    record.put(Aim.STRING.convert("PAGE_VIEW"));
                    record.put(Aim.STRING.convert("user agent info .."));
                    record.put(Aim.BYTEARRAY(2).convert("GB"));
                    record.put(Aim.BYTEARRAY(3).convert("LDN"));
                    record.put(Aim.STRING.convert("EC2 A"+ i));
                    record.put(Aim.STRING.convert("test"));
                    record.put(Aim.STRING.convert("http://"));
                    record.put(Aim.BYTEARRAY(16).convert(userUid.toString()));
                    record.put(Aim.BOOL.convert(userUid.hashCode() % 1000 == 0 ? "true" : "false"));

                    record.flip();

                    if (currentSegment.getOriginalSize() + record.limit() > table.segmentSizeBytes) {
                        addCurrentSegment();
                    }
//                    currentSegment.append(stream);
                    currentSegment.append(record);

                } catch(Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
            addCurrentSegment();
        } catch (Exception  ex) {
           ex.printStackTrace();
           System.exit(0);
        } 
    }

    private void addCurrentSegment() throws IOException {
        try {
            currentSegment.close();
            if (currentSegment.getOriginalSize()>0) {
                table.add(currentSegment);
                currentSegment = null;
            }
            createNewSegmentIfNull();
        } catch (IllegalAccessException e) {
            throw new IOException(e);
        }
    }

    private void createNewSegmentIfNull() throws IOException {
        if (currentSegment == null) {
            if (table.sortColumn == null) {
                currentSegment = new Segment(table.schema);
            } else {
                currentSegment = new SegmentSorted(table.schema, table.sortColumn, table.sortOrder);
            }
        }
    }
}
