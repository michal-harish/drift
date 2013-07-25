package net.imagini.aim;

import java.util.LinkedHashMap;


public class AimUtils {

    public static AimSchema parseSchema(String declaration) {
        final String[] dec = declaration.split(",");
        LinkedHashMap<String, AimType> result = new LinkedHashMap<String,AimType>();
        for(int f=0; f<dec.length; f++) {
            String name = String.valueOf(f+1);
            String function = null;
            Integer length = null;
            String type = dec[f].trim().toUpperCase();
            if (type.contains(":")) {
                function = dec[f].split(":")[0];
                type = dec[f].split(":")[1];
            }
            if (type.contains("[")) {
                length = Integer.valueOf(type.substring(type.indexOf("[")+1,type.indexOf("]")));
                type = type.substring(0, type.indexOf("["));
            }
            switch(type) {
                case "BOOL": result.put(name,Aim.BOOL); break;
                case "BYTE": result.put(name,Aim.BYTE); break;
                case "INT": result.put(name,Aim.INT); break;
                case "LONG": result.put(name,Aim.LONG); break;
                case "STRING": result.put(name,length==null ? Aim.STRING : Aim.STRING(length)); break;
                default: throw new IllegalArgumentException("Unknown data type " + type);
            }
            if (function != null) {
                switch(function) {
                    case "UUID": result.put(name, Aim.UUID(result.get(name).getDataType())); break;
                    case "IPV4": result.put(name, Aim.IPV4(result.get(name).getDataType())); break;
                    default: throw new IllegalArgumentException("Unknown type " + type);
                }
            }
        }
        return new AimSchema(result);
    }
}
