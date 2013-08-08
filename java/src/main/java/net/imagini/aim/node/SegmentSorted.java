package net.imagini.aim.node;


/**
 * @author mharis
 * 
 * FIXME rewrite SegmentSorted using ByteBuffer(s) instead of ByteArrayWrapper
 */
public class SegmentSorted {/*extends Segment {

    final private int sortColumn;
    final private int requiredColumns;
    final private ByteArrayOutputStream recordBuffer;
    private Map<ComparableByteArray,ByteArrayOutputStream> sortMap = new HashMap<>();
    private ComparableByteArray sortValue;
    private int recordedColumns;
    private SortOrder sortOrder;

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
        ArrayList<ComparableByteArray> keys =  new ArrayList<ComparableByteArray>(sortMap.keySet());
        Collections.sort(keys);
        if (sortOrder.equals(SortOrder.DESC)) {
            Collections.reverse(keys);
        }
        for(ComparableByteArray key: keys) {
            ByteArrayOutputStream bucket = sortMap.get(key);
            ByteArrayInputStream reader = new ByteArrayInputStream(bucket.toByteArray());
            try {
                while(true) {
                    int col= 0; for(AimType type: schema.def()) {
                        byte[] value = Pipe.read(reader, type.getDataType());
                        try {
                            AimUtils.write(type.getDataType(), value, writers.get(col++));
                        } catch (Exception e) {
                            System.err.println(type + " " + type.convert(value));
                            throw e;
                        }
                    }
                }
            } catch (EOFException e) {}
        }
        sortMap = null;
        super.close();
    }

    //TODO create utils to deal with this case - there's too much copying of byte arrays
    @Override public int write(int column, AimDataType type, byte[] value) throws IOException {
        try {
            checkWritable(true);
        } catch (IllegalAccessException e) {
            throw new IOException(e); 
        }
        if (recordedColumns++ == requiredColumns) {
            closeRecord();
        }
        if (column == sortColumn) {
            sortValue = new ComparableByteArray(value,0);
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
*/
}
