package net.imagini.aim;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import joptsimple.internal.Strings;
import net.imagini.aim.AimTypeAbstract.AimDataType;

public class AimSchema {
    private final LinkedList<AimType> def = new LinkedList<>();
    private final Map<String,Integer> colIndex = new HashMap<>();
    public AimSchema(LinkedHashMap<String,AimType> columnDefs) {
        for(Entry<String,AimType> columnDef: columnDefs.entrySet()) {
            def.add(columnDef.getValue());
            colIndex.put(columnDef.getKey(), def.size()-1);
        }
    }
    public int get(String colName) {
        return colIndex.get(colName);
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
    public String serialize() {
        String result = "";
        for(AimType type: def) {
            result += (result != "" ? "," : "") + type.toString();
        }
        return result;
    }

}
