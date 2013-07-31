package net.imagini.aim.node;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import net.imagini.aim.Aim.SortOrder;
import net.imagini.aim.AimSchema;
import net.imagini.aim.AimType;
import net.imagini.aim.AimTypeAbstract.AimDataType;
import net.imagini.aim.AimUtils;
import net.imagini.aim.ByteArrayWrapper;
import net.imagini.aim.pipes.Pipe;

import org.apache.commons.io.output.ByteArrayOutputStream;

/**
 * @author mharis
 * 
 * TODO replace ByteArrayWrapper with ByteBuffer
 */
public class SegmentSorted extends Segment {

    final private int sortColumn;
    final private int requiredColumns;
    final private ByteArrayOutputStream recordBuffer;
    private Map<ByteArrayWrapper,ByteArrayOutputStream> sortMap = new HashMap<>();
    private ByteArrayWrapper sortValue;
    private int recordedColumns;
    private SortOrder sortOrder;

    /**
     * Append-only segment
     * @throws IOException 
     */
    public SegmentSorted(AimSchema schema, int sortColumn, SortOrder sortOrder) throws IOException {
        super(schema);
        this.sortColumn = sortColumn;
        this.sortOrder = sortOrder;
        this.requiredColumns = schema.size();
        this.recordedColumns = 0;
        this.recordBuffer = new ByteArrayOutputStream();
    }

    @Override public void close() throws IOException, IllegalAccessException {
        checkWritable(true);
        closeRecord();
        ArrayList<ByteArrayWrapper> keys =  new ArrayList<ByteArrayWrapper>(sortMap.keySet());
        Collections.sort(keys);
        if (sortOrder.equals(SortOrder.DESC)) {
            Collections.reverse(keys);
        }
        for(ByteArrayWrapper key: keys) {
            ByteArrayOutputStream bucket = sortMap.get(key);
            ByteArrayInputStream reader = new ByteArrayInputStream(bucket.toByteArray());
            try {
                while(true) {
                    int col= 0; for(AimType type: schema.def()) {
                        byte[] value = Pipe.read(reader, type.getDataType());
                        AimUtils.write(type.getDataType(), value, writers.get(col++));
                    }
                }
            } catch (EOFException e) {}
        }
        sortMap = null;
        super.close();
    }

    //TODO create utils to deal with this case - there's too much copying of byte arrays
    @Override protected int write(int column, AimDataType type, byte[] value) throws IOException {
        try {
            checkWritable(true);
        } catch (IllegalAccessException e) {
            throw new IOException(e); 
        }
        if (recordedColumns++ == requiredColumns) {
            closeRecord();
        }
        if (column == sortColumn) {
            sortValue = new ByteArrayWrapper(value,0);
        }
        return AimUtils.write(type, value, recordBuffer);
    }

    private void closeRecord() throws IOException {
        if (sortValue != null)  {
            recordedColumns = 1;
            if (!sortMap.containsKey(sortValue)) {
                sortMap.put(sortValue, new ByteArrayOutputStream());
            }
            ByteArrayOutputStream keyspace = sortMap.get(sortValue);
            keyspace.write(recordBuffer.toByteArray());
            sortValue = null;
            recordBuffer.reset();
        }
    }

}
