package net.imagini.aim;

import java.util.LinkedHashMap;

public class EventsSchema extends AimSchema {

    @SuppressWarnings("serial")
    public EventsSchema() {
        super(new LinkedHashMap<String,AimDataType>() {{ 
                put("timestamp", Aim.LONG);
                put("client_ip", Aim.INT);
                put("type", Aim.STRING);
                put("user_agent", Aim.STRING);
                put("country_code", Aim.BYTEARRAY(2));
                put("region_code", Aim.BYTEARRAY(3));
                put("post_code", Aim.STRING);
                put("api_key", Aim.STRING);
                put("url", Aim.STRING);
                put("userUid", Aim.BYTEARRAY(16));
                put("userQuizzed", Aim.BOOL);
            }}
        );
    }
    

}
