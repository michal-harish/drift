package net.imagini.aim.cluster;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;
import java.util.zip.GZIPInputStream;

import net.imagini.aim.Pipe;
import net.imagini.aim.PipeLZ4;
import net.imagini.aim.Protocol;
import net.imagini.aim.types.AimSchema;
import net.imagini.aim.types.AimType;

public class StandardLoader {

    private Socket socket;
    private Pipe out;
    protected AimSchema schema;

    protected void connectTable(String host, int port, AimSchema schema)
            throws IOException {
        socket = new Socket(InetAddress.getByName(host), port);
        out = new PipeLZ4(socket.getOutputStream(), Protocol.LOADER);
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
            for (AimType type : schema.fields()) {
                out.write(type.getDataType(), record[i]);
                i++;
            }
        } catch (EOFException e) {
            out.flush();
            throw e;
        }
    }

    // public static void main(String[] args) throws IOException {
    // for (int a = 0; a < args.length; a++)
    // switch (args[a]) {
    // case "--separator":
    // separator = args[++a];
    // break;
    // case "--gzip":
    // gzip = true;
    // break;
    // case "--schema":
    // schema = AimUtils.parseSchema(args[++a]);
    // break;
    // case "--limit":
    // limit = Long.valueOf(args[++a]);
    // break;
    // default:
    // filename = args[a];
    // break;
    // }
    // CSVLoader loader = new CSVLoader(args);
    // loader.start();
    // try {
    // loader.join();
    // } catch (InterruptedException e) {
    //
    // }
    // }
    // private static void printHelp() {
    // System.out
    // .println("Usage: aim-loader [--gzip] [--limit <limit>] --schema <...>\n");
    // }

    private Boolean gzip = false;
    private InputStream in = null;
    private String separator = null;

    public StandardLoader(AimSchema schema, String host, Integer port,
            String separator, String filename, Boolean gzip) throws Exception {
        this(schema, host, port, separator, new FileInputStream(filename), gzip);
    }

    public StandardLoader(AimSchema schema, String host, Integer port,
            String separator, InputStream fileinput, Boolean gzip)
            throws Exception {
        if (schema == null) {
            throw new Exception("No schema given");
        }
        this.gzip = gzip;
        this.in = (fileinput == null) ? System.in : fileinput;

        this.separator = separator;
        connectTable(host, port, schema);
    }

    public int processInput() throws IOException {
        int count = 0;
        InputStreamReader reader;
        if (gzip)
            reader = new InputStreamReader(new GZIPInputStream(in));
        else
            reader = new InputStreamReader(in);

        try {
            BufferedReader lineReader = new BufferedReader(reader);
            String line;
            while (count++ >= 0) {
                int fields = 0;
                String[] values = new String[schema.size()];
                while (fields < schema.size()) {
                    if (null == (line = lineReader.readLine())) {
                        break;
                    }
                    for (String value : line.split(separator)) {
                        values[fields++] = value;
                    }
                }
                if (fields < schema.size()) {
                    count--;
                    break;
                }
                try {
                    byte[][] record = createEmptyRecord();
                    int i = 0;
                    for (AimType type : schema.fields()) {
                        String value = values[i];
                        record[i] = type.convert(value);
                        i++;
                    }
                    storeLoadedRecord(record);
                } catch (Exception e) {
                    System.err.println(count + ":" + values);
                    e.printStackTrace();
                }
            }
        } finally {
            out.flush();
            close();
        }
        in.close();

        return count;
    }

}
