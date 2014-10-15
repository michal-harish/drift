package net.imagini.aim.loaders;

import java.util.LinkedHashMap;

import net.imagini.aim.Aim;
import net.imagini.aim.AimSchema;
import net.imagini.aim.AimType;

public class GenericSchema extends AimSchema {

    @SuppressWarnings("serial")
    public GenericSchema() {
        super(new LinkedHashMap<String,AimType>() {{
            put("user_uid", Aim.UUID(Aim.BYTEARRAY(16)));
            put("timestamp", Aim.LONG);
            put("column", Aim.STRING);
            put("value", Aim.STRING);
        }});
    }

}

