package net.imagini.aim;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import joptsimple.internal.Strings;

/**
 * This is a virtual object which can have columns spread across
 * multiple locations.
 * 
 * @author mharis
 */
public class AimTable {

    private String name;
    private Integer segmentSize;
    private LinkedHashMap<String,AimColumn> columns; 
    private Integer count = 0;

    public AimTable(String name, Integer segmentSize, LinkedHashMap<String,AimDataType> columnDefs) {
        this.name = name;
        this.segmentSize = segmentSize;
        this.columns = new LinkedHashMap<String, AimColumn>();
        for(Entry<String,AimDataType> def: columnDefs.entrySet()) {
            this.columns.put(def.getKey(), new AimColumn(def.getValue()));
        }
    }

    public AimColumn column(String colName) {
        checkColumn(colName);
        return columns.get(colName);
    }

    private void checkColumn(String colName) throws IllegalArgumentException {
        if (!columns.containsKey(colName)) {
            throw new IllegalArgumentException(
                "Column `"+colName+"` is not defined for table `"+name+"`. Available columns are: " 
                + Strings.join(columns.keySet().toArray(new String[columns.size()-1]), ",")
            );
        }

    }

    protected void flushSegment() throws IOException {
        for(AimColumn column: columns.values()) {
            column.flushSegment();
        }
    }

    protected void closeSegment() throws IOException {
        for(AimColumn column: columns.values()) {
            column.closeSegment();
        }
    }

    public void append(Pipe pipe) throws IOException {
        for(Entry<String,AimColumn> columnEntry: columns.entrySet()) {
            AimColumn column = columnEntry.getValue();
            byte[] value = pipe.read(column.type);
            if (column.type.equals(Aim.STRING)) {
                column.write(new String(value));
            } else {
                column.write(value);
            }
        }
        count ++;
        if (count % segmentSize == 0) {
            closeSegment();
        }
    }

    public int getCount() {
        return count;
    }

    public int getNumSegments() {
        Integer i = null;
        for(AimColumn column: columns.values()) {
            if (i == null || i>column.getLastSegmentId()) i = column.getLastSegmentId();
        }
        return i + 1;
    }


}
