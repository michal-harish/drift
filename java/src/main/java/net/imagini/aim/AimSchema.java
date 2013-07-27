package net.imagini.aim;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import joptsimple.internal.Strings;
import net.imagini.aim.AimTypeAbstract.AimDataType;

public class AimSchema {
    private final LinkedList<AimType> def = new LinkedList<>();
    private final Map<String,Integer> colIndex = new LinkedHashMap<>();
    private final Map<Integer,String> nameIndex = new LinkedHashMap<>();
    public AimSchema(LinkedHashMap<String,AimType> columnDefs) {
        for(Entry<String,AimType> columnDef: columnDefs.entrySet()) {
            def.add(columnDef.getValue());
            colIndex.put(columnDef.getKey(), def.size()-1);
            nameIndex.put(def.size()-1, columnDef.getKey());
        }
    }
    public int get(String colName) {
        return colIndex.get(colName);
    }

    public AimType get(int col) {
        return def.get(col);
    }

    public AimType def(String colName) {
        return def.get(colIndex.get(colName));
    }

    public AimType[] def() {
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
        return Strings.join(colIndex.keySet().toArray(new String[def.size()-1]), ",");
    }
    public int size() {
        return def.size();
    }

    @Override public String toString() {
        String result = "";
        for(Entry<String,Integer> c: colIndex.entrySet()) {
            String name = c.getKey();
            AimType type = def.get(c.getValue());
            result += (result != "" ? "," : "") + name + "(" + type.toString() + ")";
        }
        return result;
    }
    public String[] names() {
        return new ArrayList<String>(colIndex.keySet()).toArray(new String[colIndex.size()]);
    }

    
    public AimSchema subset(final String... columns) {
        return subset(Arrays.asList(columns));
    }
    public String name(int c) {
        return nameIndex.get(c);
    }
    @SuppressWarnings("serial")
    public AimSchema subset(final List<String> columns) {
        final AimSchema subSchema = new AimSchema(new LinkedHashMap<String,AimType>() {{
            for(String colName: columns) {
                put(colName, def.get(colIndex.get(colName)));
            }
        }});
        return subSchema;
    }
}
