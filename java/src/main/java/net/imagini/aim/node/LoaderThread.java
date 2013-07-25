package net.imagini.aim.node;

import java.io.EOFException;
import java.io.IOException;

import net.imagini.aim.Aim;
import net.imagini.aim.pipes.Pipe;

public class LoaderThread extends Thread  {

    private Pipe pipe;
    private AimTable table;
    private Integer count = 0;

    public LoaderThread(AimTable table, Pipe pipe) throws IOException {
        this.pipe = pipe;
        this.table = table;
        String expectSchema = table.schema.serialize();
        String actualSchema = new String(pipe.read(Aim.STRING));
        if (!actualSchema.equals(expectSchema)) {
            throw new IOException("Invalid loader schema for table '"+ table.name +"', expecting: " + expectSchema );
        }
    }

    @Override public void run() {
        System.out.println("Loading into " + table.name);
        try {
            long t = System.currentTimeMillis();
            try {
                while (true) {
                    if (interrupted())  break;
                    table.append(pipe);
                    count++;
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
                pipe.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("Loading into " + table.name + " finished");
        }
    }
}
