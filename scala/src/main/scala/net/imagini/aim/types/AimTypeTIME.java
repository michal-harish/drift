package net.imagini.aim.types;

import net.imagini.aim.utils.ByteUtils;
import net.imagini.aim.utils.View;

import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

public class AimTypeTIME extends AimTypeLONG  {

    private final static String UTC_PATTERN = "yyyy-MM-dd HH:mm:ss";

    private final static ThreadLocal<MutableDateTime> yoda = new ThreadLocal<MutableDateTime>() {
        @Override
        protected MutableDateTime initialValue() {
            return new MutableDateTime(DateTimeZone.UTC);
        }
    };

    @Override
    public int parse(View value, byte[] dest, int destOffset) {
        int year = ByteUtils.parseIntRadix10(value.array, value.offset, value.offset + 3);
        int month = ByteUtils.parseIntRadix10(value.array, value.offset+5, value.offset + 6);
        int date = ByteUtils.parseIntRadix10(value.array, value.offset+8, value.offset + 9);
        int hrs = ByteUtils.parseIntRadix10(value.array, value.offset+11, value.offset + 12);
        int min = ByteUtils.parseIntRadix10(value.array, value.offset+14, value.offset + 15);
        int sec = ByteUtils.parseIntRadix10(value.array, value.offset+17, value.offset + 18);
        MutableDateTime d = yoda.get();
        d.setDateTime(year, month, date, hrs, min, sec, 0);
        ByteUtils.putLongValue(d.getMillis(), dest, destOffset);
        return 8;
    }

    @Override
    public String asString(byte[] src, int offset) {
        MutableDateTime d = yoda.get();
        long t = ByteUtils.asLongValue(src, offset);
        d.setDate(t);
        d.setTime(t);
        return d.toString(UTC_PATTERN);
    }
}