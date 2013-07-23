package net.imagini.aim;

import java.io.ByteArrayOutputStream;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

public class AimSegment {

    private LinkedHashMap<String,byte[]> columnar;

    //TODO public AimSegment(..mmap file)

    public AimSegment(LinkedHashMap<String,ByteArrayOutputStream> data) {
        columnar = new LinkedHashMap<String,byte[]>();
        for(Entry<String,ByteArrayOutputStream> c: data.entrySet()) {
            columnar.put(c.getKey(), c.getValue().toByteArray());
        }
    }

    public byte[] get(String column) {
        return columnar.get(column);
    }


}
