package net.imagini.aim;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import joptsimple.internal.Strings;

public class AimSchema {
    private final LinkedList<AimDataType> def = new LinkedList<>();
    private final Map<String,Integer> colIndex = new HashMap<>();
    public AimSchema(LinkedHashMap<String,AimDataType> columnDefs) {
        for(Entry<String,AimDataType> columnDef: columnDefs.entrySet()) {
            def.add(columnDef.getValue());
            colIndex.put(columnDef.getKey(), def.size()-1);
        }
    }
    public int get(String colName) {
        return colIndex.get(colName);
    }

    public AimDataType def(Integer col) {
        return def.get(col);
    }
    public AimDataType def(String colName) {
        return def.get(colIndex.get(colName));
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
}
