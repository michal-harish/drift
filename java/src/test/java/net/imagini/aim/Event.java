package net.imagini.aim;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.UUID;

import joptsimple.internal.Strings;

public class Event implements Writable {

    public final long timestamp; 
    public final InetAddress client_ip; //int32
    public final String type;
    public final String user_agent;
    public final String country_code ; //bytearray[2]
    public final String region_code;  //bytearray[3]
    public final String post_code;
    public final String api_key;
    public final String url;
    public final UUID userUid; //bytearray[16]
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
        UUID userUid,
        Boolean userQuizzed
    ) { 
        this.timestamp = timestamp;
        ByteBuffer bb = ByteBuffer.wrap(clientIp.getAddress());
        bb.order(ByteOrder.LITTLE_ENDIAN);
        this.client_ip = clientIp;
        this.type = type;
        this.user_agent = userAgent;
        this.country_code = Strings.isNullOrEmpty(countryCode) ? "??" : countryCode; 
        this.region_code = Strings.isNullOrEmpty(regionCode) ? "???" : regionCode;
        this.post_code = postCode;
        this.api_key = apiKey;
        this.url = url;
        this.userUid = userUid;
        this.userQuizzed = userQuizzed;
    }

    @Override
    public void write(Pipe out) throws IOException {
        out.write(timestamp);
        out.write(Arrays.copyOfRange(client_ip.getAddress(),0,4));
        out.write(type);
        out.write(user_agent);
        out.write(Arrays.copyOfRange(country_code.getBytes(), 0, 2));
        out.write(Arrays.copyOfRange(region_code.getBytes(), 0, 3));
        out.write(post_code);
        out.write(api_key);
        out.write(url);
        out.write(userUid.getMostSignificantBits());out.write(userUid.getLeastSignificantBits());
        out.write(userQuizzed);
    }
}
