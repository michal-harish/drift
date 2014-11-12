package net.imagini.aim.segment;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.imagini.aim.types.AimDataType;
import net.imagini.aim.types.AimSchema;
import net.imagini.aim.types.SortOrder;
import net.imagini.aim.types.TypeUtils;
import net.imagini.aim.utils.ByteKey;

/**
 * This segment type overrides the default segment behaviour by sorting the
 * record by a given sort column before compressing it.
 * 
 * @author mharis
 */
public class AimSegmentQuickSort extends AimSegment {

    final private int sortColumn;
    private Map<ByteKey, List<byte[][]>> sortMap = new HashMap<>();
    private SortOrder sortOrder;
    private AimDataType sortDataType;

    public AimSegmentQuickSort(AimSchema schema) {
        super(schema);
        this.sortColumn = 0;
        this.sortOrder = SortOrder.ASC;
        this.sortDataType = schema.dataType(sortColumn);
    }

    @Override
    public AimSegment appendRecord(byte[][] record) throws IOException {
        try {
            checkWritable(true);
            ByteKey sortValue = new ByteKey(record[0], TypeUtils.sizeOf(sortDataType, record[0]), 0);
            int recordSize = 0; 
            for(int i=0; i<record.length; i ++) {
                recordSize += record[i].length;
            }
            if (sortValue != null) {
                if (!sortMap.containsKey(sortValue)) {
                    sortMap.put(sortValue, new LinkedList<byte[][]>());
                }
                List<byte[][]> keyspace = sortMap.get(sortValue);
                keyspace.add(record);
                originalSize.addAndGet(recordSize);
            }
            return this;
        } catch (IllegalAccessException e) {
            throw new IOException(e);
        }
    }

    @Override
    public AimSegment close() throws IOException, IllegalAccessException {
        checkWritable(true);
        List<ByteKey> keys = new ArrayList<ByteKey>(sortMap.keySet());
        Collections.sort(keys);
        if (sortOrder.equals(SortOrder.DESC)) {
            Collections.reverse(keys);
        }
        originalSize.set(0);
        for (ByteKey key : keys) {
            List<byte[][]> bucket = sortMap.get(key);
            for (byte[][] record : bucket) {
                try {
                    commitRecord(record);
                } catch (EOFException e) {
                    break;
                }
            }
        }
        sortMap = null;
        return super.close();
    }

}
