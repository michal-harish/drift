package net.imagini.drift.types;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import net.imagini.drift.utils.Tokenizer;
import net.imagini.drift.utils.View;

import org.apache.commons.lang3.StringUtils;

public class DriftSchema {

    public static DriftSchema fromString(String declaration) {
        return fromTokens(Tokenizer.tokenize(declaration, true));
    }

    public static DriftSchema fromTokens(Queue<String> tokens) {
        LinkedHashMap<String, DriftType> result = new LinkedHashMap<String, DriftType>();
        while (!tokens.isEmpty()) {
            String fieldName = tokens.poll();
            DriftType driftType;
            if (tokens.peek().equals("(")) {
                tokens.poll();
                String typeDeclaration = tokens.poll();
                switch (typeDeclaration) {
                case "BOOL":
                    driftType = Drift.BOOL;
                    break;
                case "BYTE":
                    driftType = Drift.BYTE;
                    break;
                case "INT":
                    driftType = Drift.INT;
                    break;
                case "LONG":
                    driftType = Drift.LONG;
                    break;
                case "BYTEARRAY":
                    if (!tokens.poll().equals("["))
                        throw new IllegalArgumentException(
                                "Invalid schema declaration, missing '['");
                    driftType = Drift.BYTEARRAY(Integer.valueOf(tokens.poll()));
                    if (!tokens.poll().equals("]"))
                        throw new IllegalArgumentException(
                                "Invalid schema declaration, missing ']'");
                    break;
                case "STRING":
                    driftType = Drift.STRING;
                    break;
                case "UUID":
                    driftType = Drift.UUID;
                    break;
                case "IPV4":
                    driftType = Drift.IPV4;
                    break;
                case "TIME":
                    driftType = Drift.TIME;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown data type " + typeDeclaration);
                }
                if (!tokens.poll().equals(")"))
                    throw new IllegalArgumentException(
                            "Invalid schema declaration, missing ')'");
            } else {
                driftType = Drift.STRING;
            }
            result.put(fieldName, driftType);
            if (!tokens.isEmpty() && !tokens.poll().equals(",")) {
                throw new IllegalArgumentException(
                        "Invalid schema declaration, missing ','");
            }
        }
        return new DriftSchema(result);
    }

    private final LinkedList<DriftType> def = new LinkedList<>();
    private final Map<String, Integer> colIndex = new LinkedHashMap<>();
    private final Map<Integer, String> nameIndex = new LinkedHashMap<>();

    public DriftSchema(LinkedHashMap<String, DriftType> columnDefs) {
        for (Entry<String, DriftType> columnDef : columnDefs.entrySet()) {
            def.add(columnDef.getValue());
            colIndex.put(columnDef.getKey(), def.size() - 1);
            nameIndex.put(def.size() - 1, columnDef.getKey());
        }
    }

    public String asString(View[] view) {
        return asString(view, ", ");
    }

    public String asString(View[] view, String separator) {
        String result = "";
        for (int i = 0; i < size(); i++) {
            DriftType t = def.get(i);
            result += t.asString(view[i]);
            if (t != def.get(size() - 1))
                result += separator;
        }
        return result;
    }

    public String[] asStrings(View[] view) {
        String[] result = new String[view.length];
        for (int i = 0; i < size(); i++) {
            DriftType t = def.get(i);
            result[i] = t.asString(view[i]);
        }
        return result;
    }

    public String asString(View view) {
        return asString(view, ", ");
    }

    public String asString(View view, String separator) {
        int mark = view.offset;
        String result = "";
        for (int f = 0; f < size(); f++) {
            result += def.get(f).asString(view);
            view.offset += def.get(f).sizeOf(view);
            if (f < size() - 1)
                result += separator;
        }
        view.offset = mark;
        return result;
    }

    public int get(String colName) {
        return colIndex.get(colName);
    }

    public DriftType get(int col) {
        return def.get(col);
    }

    public DriftType field(String colName) {
        return def.get(colIndex.get(colName));
    }

    public DriftType[] fields() {
        return def.toArray(new DriftType[def.size()]);
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
            DriftType type = def.get(c.getValue());
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

    public DriftSchema subset(final String[] columns) {
        return subset(Arrays.asList(columns));
    }

    @SuppressWarnings("serial")
    public DriftSchema subset(final List<String> columns) {
        final DriftSchema subSchema = new DriftSchema(
                new LinkedHashMap<String, DriftType>() {
                    {
                        for (String colName : columns)
                            if (colName != null) {
                                if (!colIndex.containsKey(colName)) {
                                    throw new DriftQueryException(
                                            "Unknown field " + colName);
                                }
                                put(colName, def.get(colIndex.get(colName)));
                            }
                    }
                });
        return subSchema;
    }
}
