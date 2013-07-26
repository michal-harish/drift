package net.imagini.aim.loaders;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.zip.GZIPInputStream;

import net.imagini.aim.AimSchema;
import net.imagini.aim.AimType;
import net.imagini.aim.AimUtils;
import net.imagini.aim.pipes.Pipe;
import net.imagini.aim.pipes.PipeLZ4;


/**
 * 
 * @author mharis
 *  
 * mvn package
 * 
 * ## AND THEN
 * hive -e "select \`timestamp\`,client_ip,\`type\`,useragent,country_code,region_code,post_code,campaignid,url,userUid,userQuizzed from ${env:USER}_events_rc;" \
 *  | gzip > ~/events-2013-07-23.csv.gz 
 * cat ~/events-2013-07-23.csv.gz | ./load-csv --gzip --schema timestamp(LONG),client_ip(IPV4:INT),event_type(STRING),user_agent(STRING),country_code(STRING[2]),region_code(STRING[3]),post_code(STRING),api_key(STRING),url(STRING),user_uid(UUID:STRING[16]),user_quizzed(BOOL)
 *
 * ## OR
 * 
 * hive -e "..." | ./load-csv --schema timestamp(LONG),client_ip(IPV4:INT),event_type(STRING),user_agent(STRING),country_code(STRING[2]),region_code(STRING[3]),post_code(STRING),api_key(STRING),url(STRING),user_uid(UUID:STRING[16]),user_quizzed(BOOL)
 */
public class CSVLoader {
    public static void main(String[] args) throws UnknownHostException, IOException {
        Boolean gzip = false;
        AimSchema schema = null;
        String filename = null;
        for(int a=0; a<args.length; a++) switch(args[a]) {
            case "--gzip": gzip = true; break;
            case "--schema": schema = AimUtils.parseSchema(args[++a]); break;
            default: filename = args[a]; break;
        }
        if (schema == null) {
            printHelp();
            System.exit(1);
        }

        InputStream in;
        if (filename == null) {
            in = System.in;
        } else {
            in = new FileInputStream(filename); 
        }

        InputStreamReader reader;
        if (gzip) reader = new InputStreamReader(new GZIPInputStream(in));
        else reader = new InputStreamReader(in);

        Socket socket = new Socket(InetAddress.getByName("localhost"), 4000);
        final Pipe out = new PipeLZ4(socket.getOutputStream(),Pipe.Protocol.LOADER);
        out.write(schema.toString());
        try {
            BufferedReader lineReader = new BufferedReader(reader);
            while(true) {
                String line = lineReader.readLine();
                String[] values= line.split("\t");
                try {
                    int i = 0; for(AimType type: schema.def()) {
                        String value = values[i++];
                        //System.out.println(type + " >> " + value);
                        out.write(type.getDataType(), type.convert(value));
                    }
                } catch (SocketException e) {
                    e.printStackTrace();
                    break;
                } catch (EOFException e) {
                    break;
                } catch (Exception e) {
                    System.out.println(line);
                    e.printStackTrace();
                }
            }
        } finally {
            out.close();
            socket.close();
        }
        in.close();
    }

    private static void printHelp() {
        // TODO Auto-generated method stub
        
    }

}
