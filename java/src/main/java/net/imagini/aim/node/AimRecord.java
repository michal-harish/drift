package net.imagini.aim.node;

import java.io.IOException;

import net.imagini.aim.AimSchema;
import net.imagini.aim.AimType;
import net.imagini.aim.pipes.Pipe;

import org.apache.commons.io.output.ByteArrayOutputStream;

public class AimRecord implements Comparable<AimRecord> {
    final byte[][] data;
    final private AimSchema schema;
    private byte[] sortValue;

    public AimRecord(AimSchema schema, Pipe in, Integer sortColumn ) throws IOException {
        this.schema = schema;
        this.sortValue = null;
        data = new byte[schema.size()][];
        int i = 0; for (AimType type : schema.def()) {
            byte[] value = in.read(type.getDataType());
            data[i] = value;
            if (sortColumn!= null && i == sortColumn) sortValue = data[i];
            i++;
        }
    }

    public AimRecord(AimSchema schema, byte[][] data, Integer sortColumn ) {
        this.schema = schema;
        this.data = data;
        if (sortColumn!= null) sortValue = data[sortColumn];
    }

    @Override
    public int compareTo(AimRecord o) {
        if (o.sortValue == null || sortValue == null) {
            throw new IllegalStateException("Record is in a detached state");
        }
        if (sortValue.length < o.sortValue.length) {
            return -1;
        } else if (sortValue.length > o.sortValue.length) {
            return 1;
        }
        int result = 0;
        int i = 0;
        while (i<sortValue.length) {
            if ((sortValue[i] & 0xFF) < (o.sortValue[i] & 0xFF)) {
                return -1;
            } else if ((sortValue[i] & 0xFF) > (o.sortValue[i] & 0xFF) ) {
                return  1;
            }
            i++;
        }
        return result;
    }

    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        int i = 0 ; 
        for(AimType type: schema.def()) {
            Pipe.write(type.getDataType(), data[i++], os);
        }
        return os.toByteArray();
    }

}
