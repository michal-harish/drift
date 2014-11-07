package net.imagini.aim.types;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import net.imagini.aim.utils.ByteUtils;
import net.imagini.aim.utils.View;

public class AimTypeTIME extends AimTypeAbstract {

    private AimDataType dataType;

    public AimTypeTIME(AimDataType dataType) {
        if (!dataType.equals(Aim.LONG) && !dataType.equals(Aim.STRING)) {
            throw new IllegalArgumentException("Unsupported data type `"
                    + dataType + "` for type AimTypeTime");
        }
        this.dataType = dataType;
    }

    @Override
    public AimDataType getDataType() {
        return dataType;
    }

    @Override
    public String toString() {
        return "TIME:" + dataType.toString();
    }

    @Override
    public byte[] convert(String value) {
        if (value != null && !value.isEmpty()) {
            if (dataType.equals(Aim.STRING)) {
                return Aim.STRING.convert(value);
            } else if (dataType.equals(Aim.LONG)) {
                try {
                    //TODO joda time for thread-safe formatting and parsing
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd H:mm:ss");
                    Date d = formatter.parse(value);
                    return Aim.LONG.convert(String.valueOf(d.getTime()));
                } catch (ParseException e) {
                    throw new AimQueryException("Could not parse timestamp " + value);
                }
            }
        }
        return new byte[0];
    }

    @Override
    public String convert(byte[] value) {
        if (dataType.equals(Aim.STRING)) {
            return dataType.convert(value);
        } else if (dataType.equals(Aim.LONG)) {
            DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd H:mm:ss");
            return formatter.format(Long.valueOf(Aim.LONG.convert(value)));
        }
        throw new IllegalArgumentException();
    }

    @Override public String asString(View view) {
        if (dataType.equals(Aim.STRING)) {
            return dataType.asString(view);
        } else if (dataType.equals(Aim.LONG)) {
            DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd H:mm:ss");
            return formatter.format(ByteUtils.asLongValue(view));
        }
        throw new IllegalArgumentException();
    }
}