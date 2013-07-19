package net.imagini.aim;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class Event {

    public final long timestamp;
    public final int client_ip;
    public final String type;
    public final String user_agent;
    public final byte[] country_code ; //2
    public final byte[] region_code;  //3
    public final String post_code;
    public final String api_key;
    public final String url;
    public final String userUid; //16
    public final boolean userQuizzed;
    
    public Event(
        Long timestamp,
        InetAddress clientIp,
        String type,
        String userAgent,
        String countryCode,
        String regionCode,
        String postCode,
        String apiKey,
        String url,
        String userUid,
        Boolean userQuizzed
    ) {
        this.timestamp = timestamp;
        ByteBuffer bb = ByteBuffer.wrap(clientIp.getAddress());
        bb.order(ByteOrder.LITTLE_ENDIAN);
        this.client_ip = bb.getInt();
        this.type = type;
        this.user_agent = userAgent;
        this.country_code = Arrays.copyOfRange(countryCode.getBytes(), 0, 2);
        this.region_code = Arrays.copyOfRange(regionCode.getBytes(), 0, 3);
        this.post_code = postCode;
        this.api_key = apiKey;
        this.url = url;
        this.userUid = userUid.toString();
        this.userQuizzed = userQuizzed;
    }
    
    public void write(AimPipe out) throws IOException {
        out.write(timestamp);
        out.write(client_ip);
        out.write(type);
        out.write(user_agent);
        out.write(country_code);
        out.write(region_code);
        out.write(post_code);
        out.write(api_key);
        out.write(url);
        out.write(userUid);
        out.write(userQuizzed);
    }
}
