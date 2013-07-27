package net.imagini.aim.node;

import java.io.EOFException;
import java.io.IOException;

import net.imagini.aim.Aim;
import net.imagini.aim.AimSegment;
import net.imagini.aim.pipes.Pipe;

public class TableServerLoaderThread extends Thread  {

    private Pipe pipe;
    private AimTable table;
    private Integer count = 0;
    private AimSegment currentSegment; 

    public TableServerLoaderThread(AimTable table, Pipe pipe) throws IOException {
        this.pipe = pipe;
        this.table = table;
        String expectSchema = table.schema.toString();
        String actualSchema = new String(pipe.read(Aim.STRING));
        if (!actualSchema.equals(expectSchema)) {
            throw new IOException("Invalid loader schema for table '"+ table.name +"', \nexpecting: " + expectSchema +"\nreceived:  " + actualSchema);
        }
    }

    @Override public void run() {
        System.out.println("Loading into " + table.name);
        try {
            long t = System.currentTimeMillis();
            try {
                while (true) {
                    if (interrupted())  break;
                    append(pipe);
                }
            } catch (EOFException e) {
                System.out.println("load(EOF) records: " + count
                        + " time(ms): " + (System.currentTimeMillis() - t)
                );
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                commitCurrentSegment();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("Loading into " + table.name + " finished");
            try { pipe.close(); } catch (IOException e) { }
        }
    }

    private synchronized void append(Pipe pipe) throws IOException {
        if (currentSegment == null || currentSegment.getCount() % table.segmentSize == 0) {
            //FIXME add sortColumn to schema and if set instantiate sorted
            //currentSegment = new Segment(table.schema);
            currentSegment = new SegmentSorted(table.schema, table.sortColumn, table.sortOrder);
        }
        try {
            currentSegment.append(pipe);
            if (currentSegment.getCount() % table.segmentSize == 0) {
                commitCurrentSegment();
            }
        } catch (EOFException e) {
            commitCurrentSegment();
            throw e;
        }
    }

    private void commitCurrentSegment() throws IOException {
        //TODO thread-safe swap
        if (currentSegment != null) {
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
