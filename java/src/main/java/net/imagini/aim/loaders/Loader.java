package net.imagini.aim.loaders;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import net.imagini.aim.AimSchema;
import net.imagini.aim.AimType;
import net.imagini.aim.Pipe;
import net.imagini.aim.PipeLZ4;

public abstract class Loader extends Thread {
    private Socket socket;
    private Pipe out;
    protected AimSchema schema;

    protected void connectTable(String host, int port, AimSchema schema) throws IOException {
        socket = new Socket(InetAddress.getByName(host), port);
        out = new PipeLZ4(socket.getOutputStream(), Pipe.Protocol.LOADER);
        out.write(schema.toString());
        this.schema = schema;
    }
    protected void close() throws IOException {
        out.close();
        socket.close();
    }

    protected byte[][] createEmptyRecord() {
        return new byte[schema.size()][];
    }

    protected void storeLoadedRecord(byte[][] record) throws IOException {
        try {
            int i = 0;
            for (AimType type : schema.def()) {
                out.write(type.getDataType(), record[i]);
                i++;
            }
        } catch (EOFException e) {
            out.flush();
            throw e;
        }
    }
}
