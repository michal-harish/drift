package net.imagini.aim.node;

import java.util.LinkedHashMap;

import net.imagini.aim.Aim;
import net.imagini.aim.AimSchema;
import net.imagini.aim.AimType;

public class EventsSchema extends AimSchema {

    @SuppressWarnings("serial")
    public EventsSchema() {
        super(new LinkedHashMap<String,AimType>() {{ 
                put("timestamp", Aim.LONG);
                put("client_ip", Aim.IPV4(Aim.INT));
                put("event_type", Aim.STRING);
                put("user_agent", Aim.STRING);
                put("country_code", Aim.STRING(2));
                put("region_code", Aim.STRING(3));
                put("post_code", Aim.STRING);
                put("api_key", Aim.STRING);
                put("url", Aim.STRING);
                put("user_uid", Aim.UUID(Aim.STRING(16)));
                put("user_quizzed", Aim.BOOL);
            }}
        );
    }
    

}
