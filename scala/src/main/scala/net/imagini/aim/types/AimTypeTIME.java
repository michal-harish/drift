package net.imagini.aim.types;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import net.imagini.aim.utils.ByteUtils;
import net.imagini.aim.utils.View;

public class AimTypeTIME extends AimTypeLONG  {

    @Override
    public int convert(String value, byte[] dest, int destOffset) {
        if (value == null || value.isEmpty()) {
            return super.convert("0", dest, destOffset);
        } else {
            try {
                // TODO joda time for thread-safe formatting and parsing
                SimpleDateFormat formatter = new SimpleDateFormat(
                        "yyyy-MM-dd H:mm:ss");
                Date d = formatter.parse(value);
                ByteUtils.putLongValue(d.getTime(), dest, destOffset);
                return 8;
            } catch (ParseException e) {
                throw new AimQueryException("Could not parse timestamp "
                        + value);
            }
        }
    }

    @Override
    public byte[] convert(String value) {
        if (value == null || value.isEmpty()) {
            return new byte[0];
        } else {
            try {
                // TODO joda time for thread-safe formatting and parsing
                SimpleDateFormat formatter = new SimpleDateFormat(
                        "yyyy-MM-dd H:mm:ss");
                Date d = formatter.parse(value);
                byte[] b = new byte[8];
                ByteUtils.putLongValue(d.getTime(), b, 0);
                return b;
            } catch (ParseException e) {
                throw new AimQueryException("Could not parse timestamp "
                        + value);
            }
        }
    }

    @Override
    public String convert(byte[] value) {
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd H:mm:ss");
        return formatter.format(ByteUtils.asLongValue(value, 0));
    }

    @Override
    public String asString(View view) {
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd H:mm:ss");
        return formatter.format(ByteUtils.asLongValue(view.array, view.offset));
    }
}