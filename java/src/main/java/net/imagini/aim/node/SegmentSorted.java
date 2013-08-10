package net.imagini.aim.node;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.imagini.aim.Aim.SortOrder;
import net.imagini.aim.AimSchema;
import net.imagini.aim.AimTypeAbstract.AimDataType;
import net.imagini.aim.AimUtils;
import net.imagini.aim.ByteKey;


/**
 * This segment type overrides the default segment behaviour 
 * by sorting the record by a given sort column before compressing it.
 * 
 * @author mharis
 */
public class SegmentSorted extends Segment {

    final private int sortColumn;
    private Map<ByteKey,List<ByteBuffer>> sortMap = new HashMap<>();
    private SortOrder sortOrder;

    public SegmentSorted(AimSchema schema, int sortColumn, SortOrder sortOrder) throws IOException {
        super(schema);
        this.sortColumn = sortColumn;
        this.sortOrder = sortOrder;
    }

    @Override public void close() throws IOException, IllegalAccessException {
        checkWritable(true);
        List<ByteKey> keys =  new ArrayList<ByteKey>(sortMap.keySet());
        Collections.sort(keys);
        if (sortOrder.equals(SortOrder.DESC)) {
            Collections.reverse(keys);
        }
        originalSize.set(0L);
        for(ByteKey key: keys) {
            List<ByteBuffer> bucket = sortMap.get(key);
            for(ByteBuffer record: bucket) {
                try {
                    super.append(record);
                } catch (EOFException e) {
                    break;
                }
            }
        }
        sortMap = null;
        super.close();
    }

    @Override public void append(ByteBuffer record) throws IOException {
        try {
            checkWritable(true);

            ByteKey sortValue = null;
            for(int col = 0; col < schema.size() ; col++) {
                AimDataType type = schema.dataType(col);

                if (col == sortColumn) {
                    sortValue = new ByteKey(
                        Arrays.copyOfRange(record.array()
                        ,record.position()
                        ,record.position() + AimUtils.size(record, type)
                        )
                    );
                }

                originalSize.addAndGet(
                    AimUtils.skip(record, type)
                );

            }
            //close record
            if (sortValue != null)  {
                if (!sortMap.containsKey(sortValue)) {
                    sortMap.put(sortValue, new ArrayList<ByteBuffer>());
                }
                List<ByteBuffer> keyspace = sortMap.get(sortValue);
                keyspace.add(ByteBuffer.wrap(Arrays.copyOfRange(record.array(),0,record.limit())));
            }

        } catch (IllegalAccessException e) {
            throw new IOException(e); 
        }
    }
}
