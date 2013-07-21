package net.imagini.aim;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import joptsimple.internal.Strings;

/**
 * This is a virtual object which can have columns spread across
 * mutliple locations.
 * 
 * @author mharis
 */
public class AimTable {

    String name;
    Integer segmentSize;
    LinkedHashMap<String,AimColumn> columns; 
    Integer count = 0;

    public AimTable(String name, Integer segmentSize, LinkedHashMap<String,AimDataType> columnDefs) {
        this.name = name;
        this.segmentSize = segmentSize;
        this.columns = new LinkedHashMap<String, AimColumn>();
        for(Entry<String,AimDataType> def: columnDefs.entrySet()) {
            this.columns.put(def.getKey(), new AimColumn(def.getValue()));
        }
    }

    public Pipe[] select(String... colNames) throws IOException {
        Pipe[] result = new Pipe[colNames.length];
        int i = 0; for(String colName: colNames) {
            checkColumn(colName);
            AimColumn column = columns.get(colName);
            result[i++] = new Pipe(column.select(column.getLastSegmentId()));
        }
        return result;
    }

    private void checkColumn(String colName) {
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
            //TODO log.debug("Loading " + columnEntry.getKey() + " " + column.type.toString());
            if (column.type.equals(Aim.BOOL)) {
                column.write( pipe.readByteArray(1) ); 
            } else if (column.type.equals(Aim.BYTE)) {
                column.write( pipe.readByteArray(1) );
            } else if (column.type.equals(Aim.INT)) {
                column.write( pipe.readByteArray(4) );
            } else if (column.type.equals(Aim.LONG)) {
                column.write( pipe.readByteArray(8) );
            } else if (column.type.equals(Aim.STRING)) {
                column.write( pipe.readString() );
            } else if (column.type instanceof Aim.BYTEARRAY) {
                column.write( pipe.readByteArray(((Aim.BYTEARRAY)column.type).size) );
            } else {
                throw new IOException("Unknown column.type " + column.type);
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
            if (i == null) i = column.getLastSegmentId();
            else if (!i.equals(column.getLastSegmentId())) {
                throw new IllegalStateException();
            }
        }
        return i + 1;
    }

    public String readAsText(String colName, Pipe pipe) throws IOException {
        AimColumn column = columns.get(colName);
        if (column.type.equals(Aim.BOOL)) {
            return String.valueOf(pipe.readBool());
        } else if (column.type.equals(Aim.BYTE)) {
            return String.valueOf(pipe.readByte());
        } else if (column.type.equals(Aim.INT)) {
            return String.valueOf(pipe.readInt32());
        } else if (column.type.equals(Aim.LONG)) {
            return String.valueOf(pipe.readLong());
        } else if (column.type.equals(Aim.STRING)) {
            return pipe.readString();
        } else if (column.type instanceof Aim.BYTEARRAY) {
            return new String(pipe.readByteArray(((Aim.BYTEARRAY)column.type).size));
        } else {
            throw new IOException("Unknown column.type " + column.type);
        }
    }


}
