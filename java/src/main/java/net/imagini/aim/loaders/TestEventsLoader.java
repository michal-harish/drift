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
    private ByteBuffer currentBlock = ByteBuffer.allocate(1000000);
    private long limit;

    public TestEventsLoader(AimTable table, long limit) {
        this.table = table;
        this.limit = limit;
    }

    public void run() {
        try {
            currentBlock.order(ByteOrder.BIG_ENDIAN);
            for (long i = 1; i <= limit; i++) {
                if (currentSegment == null) {
                    //currentSegment = new SegmentSorted(table.schema,table.sortColumn, table.sortOrder);
                    currentSegment = new Segment(table.schema);
                }
                try {
                    UUID userUid  = UUID.randomUUID();
                    currentBlock.putLong(i); 
                    currentBlock.put(Aim.IPV4(Aim.INT).convert("173.194.41.99"));
                    currentBlock.put(Aim.STRING.convert("VDNAUserTestEvent"));
                    currentBlock.put(Aim.STRING.convert("user agent info .."));
                    currentBlock.put(Aim.BYTEARRAY(2).convert("GB"));
                    currentBlock.put(Aim.BYTEARRAY(3).convert("LDN"));
                    currentBlock.put(Aim.STRING.convert("EC2 A"+ i));
                    currentBlock.put(Aim.STRING.convert("test"));
                    currentBlock.put(Aim.STRING.convert("http://"));
                    currentBlock.put(Aim.BYTEARRAY(16).convert(userUid.toString()));
                    currentBlock.put(Aim.BOOL.convert(userUid.hashCode() % 100 == 0 ? "true" : "false"));
                    commitCurrentSegment(false);
                } catch(Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
            commitCurrentSegment(true);
        } catch (Exception  ex) {
           ex.printStackTrace();
           System.exit(0);
        } 
    }

    private void commitCurrentSegment(boolean force) throws IOException {
        if (currentSegment != null) {
            boolean commit = force || currentSegment.getOriginalSize() + currentBlock.limit() > table.segmentSizeBytes;
            if (commit || currentBlock.position() > Aim.LZ4_BLOCK_SIZE) {
                currentBlock.flip();
                currentSegment.append(currentBlock);
                currentBlock.clear();
            }
            if (commit) {
                try {
                    if (currentSegment.getOriginalSize() > 0) {
                        table.add(currentSegment);
                    }
                    currentSegment.close();
                    currentSegment = null;
                } catch (IllegalAccessException e) {
                    throw new IOException(e);
                }
            }
        }
    }
}
