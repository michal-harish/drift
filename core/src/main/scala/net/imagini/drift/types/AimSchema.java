package net.imagini.drift.types;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.imagini.drift.utils.View;

import org.apache.commons.lang3.StringUtils;

public class AimSchema {

    public static AimSchema fromString(String declaration) {
        final String[] dec = declaration.split(",");
        LinkedHashMap<String, AimType> result = new LinkedHashMap<String, AimType>();
        for (int f = 0; f < dec.length; f++) {
            String name = String.valueOf(f + 1);
            Integer arg = null;
            String type = dec[f].trim();
            if (type.contains("(")) {
                name = type.substring(0, type.indexOf("("));
                type = type.substring(type.indexOf("(") + 1, type.indexOf(")"));
            }
            type = type.toUpperCase();
            if (type.contains("[")) {
                arg = Integer.valueOf(type.substring(type.indexOf("[") + 1,
                        type.indexOf("]")));
                type = type.substring(0, type.indexOf("["));
            }
            switch (type) {
            case "BOOL":
                result.put(name, Aim.BOOL);
                break;
            case "BYTE":
                result.put(name, Aim.BYTE);
                break;
            case "INT":
                result.put(name, Aim.INT);
                break;
            case "LONG":
                result.put(name, Aim.LONG);
                break;
            case "BYTEARRAY":
                result.put(name, Aim.BYTEARRAY(arg));
                break;
            case "STRING":
                result.put(name, Aim.STRING);
                break;
            case "UUID":
                result.put(name, Aim.UUID);
                break;
            case "IPV4":
                result.put(name, Aim.IPV4);
                break;
            case "TIME":
                result.put(name, Aim.TIME);
                break;
            default:
                throw new IllegalArgumentException("Unknown data type " + type);
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

    public String asString(View[] view) {
        return asString(view, ", ");
    }
    public String asString(View[] view, String separator) {
        String result ="";
        for(int i=0; i <size(); i++) {
            AimType t= def.get(i);
            result +=t.asString(view[i]);
            if (t != def.get(size()-1)) result += separator;
        }
        return result;
    }
    public String[] asStrings(View[] view) {
        String[] result = new String[view.length];
        for(int i=0; i <size(); i++) {
            AimType t= def.get(i);
            result[i] = t.asString(view[i]);
        }
        return result;
    }
    public String asString(View view) {
        return asString(view, ", ");
    }
    public String asString(View view, String separator) {
        int mark = view.offset;
        String result ="";
        for(int f = 0; f<size(); f++) {
            result += def.get(f).asString(view);
            view.offset += def.get(f).sizeOf(view);
            if (f <size() -1) result += separator;
        }
        view.offset = mark;
        return result;
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
        // String result = name + "=";
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

    public String name(int c) {
        return nameIndex.get(c);
    }

    public AimSchema subset(final String[] columns) {
        return subset(Arrays.asList(columns));
    }

    @SuppressWarnings("serial")
    public AimSchema subset(final List<String> columns) {
        final AimSchema subSchema = new AimSchema(
            new LinkedHashMap<String, AimType>() {
                {
                    for (String colName : columns)
                        if (colName != null) {
                            if (!colIndex.containsKey(colName)) {
                                throw new AimQueryException("Unknown field " + colName);
                            }
                            put(colName, def.get(colIndex.get(colName)));
                        }
                }
            });
        return subSchema;
    }
}
