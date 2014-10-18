package net.imagini.aim;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.imagini.aim.types.Aim;
import net.imagini.aim.types.AimDataType;
import net.imagini.aim.types.AimType;

import org.apache.commons.lang3.StringUtils;

public class AimSchema {

    public static AimSchema parseSchema(String declaration) {
//        String schemaName = "new_schema";
//        if (declaration.matches("^[a-z_A-Z0-9]=")) {
//            schemaName = declaration.substring(0,declaration.indexOf("=")-1);
//            declaration = declaration.substring(schemaName.length() + 1);
//        }
        final String[] dec = declaration.split(",");
        LinkedHashMap<String, AimType> result = new LinkedHashMap<String,AimType>();
        for(int f=0; f<dec.length; f++) {
            String name = String.valueOf(f+1);
            String function = null;
            Integer length = null;
            String type = dec[f].trim();
            if (type.contains("(")) {
                name = type.substring(0, type.indexOf("("));
                type = type.substring(type.indexOf("(")+1,type.indexOf(")"));
            }
            type = type.toUpperCase();
            if (type.contains(":")) {
                function = type.split(":")[0];
                type = type.split(":")[1];
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
                case "BYTEARRAY": result.put(name,Aim.BYTEARRAY(length)); break;
                case "STRING": result.put(name,Aim.STRING); break;
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

    private final LinkedList<AimType> def = new LinkedList<>();
    private final Map<String, Integer> colIndex = new LinkedHashMap<>();
    private final Map<Integer, String> nameIndex = new LinkedHashMap<>();

    public AimSchema(LinkedHashMap<String, AimType> columnDefs) {
        for (Entry<String, AimType> columnDef : columnDefs.entrySet()) {
            def.add(columnDef.getValue());
            colIndex.put(columnDef.getKey(), def.size() - 1);
            nameIndex.put(def.size() - 1, columnDef.getKey());
        }
    }

    public int get(String colName) {
        return colIndex.get(colName);
    }

    public AimType get(int col) {
        return def.get(col);
    }

    public AimType field(String colName) {
        return def.get(colIndex.get(colName));
    }

    public AimType[] fields() {
        return def.toArray(new AimType[def.size()]);
    }

    public AimDataType dataType(Integer col) {
        return def.get(col).getDataType();
    }

    public AimDataType dataType(String colName) {
        return def.get(colIndex.get(colName)).getDataType();
    }

    public boolean has(String colName) {
        return colIndex.containsKey(colName);
    }

    public String describe() {
        return StringUtils.join(
                colIndex.keySet().toArray(new String[def.size() - 1]), ",");
    }

    public int size() {
        return def.size();
    }

    @Override
    public String toString() {
//        String result = name + "=";
        String result = "";
        for (Entry<String, Integer> c : colIndex.entrySet()) {
            String name = c.getKey();
            AimType type = def.get(c.getValue());
            result += (result != "" ? "," : "") + name + "(" + type.toString()
                    + ")";
        }
        return result;
    }

    public String[] names() {
        return new ArrayList<String>(colIndex.keySet())
                .toArray(new String[colIndex.size()]);
    }

    public AimSchema subset(final String... columns) {
        return subset(Arrays.asList(columns));
    }

    public String name(int c) {
        return nameIndex.get(c);
    }

    @SuppressWarnings("serial")
    public AimSchema subset(final List<String> columns) {
        final AimSchema subSchema = new AimSchema(
                new LinkedHashMap<String, AimType>() {
                    {
                        for (String colName : columns) {
                            put(colName, def.get(colIndex.get(colName)));
                        }
                    }
                });
        return subSchema;
    }
}
