package net.imagini.aim.utils;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class CSVStreamParser {

    private InputStream input;
    private char separator;
    private byte[] buffer = new byte[65535];
    private int position = -1;
    private int limit = -1;

    private static final String EMPTY_STRING = "";

    public CSVStreamParser(InputStream input, char separator) {
        this.input = input;
        this.separator = separator;
    }

    public String nextValue() throws IOException {
        int start = -1;
        int end = -1;
        char ch;
        try {
            while (true) {
                ++position;
                if (position >= limit ) {
                    if (start >= 0 && end >= 0) {
                        ByteUtils.copy(buffer, start, buffer, 0, end - start + 1);
                        position = end - start + 1;
                        start = 0;
                    } else {
                        position = 0;
                        start = -1; 
                    }
                    loadBuffer();
                    end = position - 1;
                }
                ch = (char) buffer[position];
                if (start == -1) {
                    if (ch == '\r' || ch == ' ') {
                        continue;
                    } else {
                        start = position;
                    }
                }
                if (ch == separator || ch == '\n') {
                    break;
                } else {
                    if (ch != ' ' && ch != '\r' && ch != 0) {
                        end = position;
                    }
                }
            }
        } catch (EOFException e) {
            if (start == -1) {
                input.close();
                throw e;
            } 
        }
//        bufferView.offset = start;
//        bufferView.limit = end;
//        return bufferView;
        return end >= start ? new String(buffer, start, end - start + 1) : EMPTY_STRING;
    }

    private void loadBuffer() throws IOException {
        limit = position + input.read(buffer, position, buffer.length - position);
        if (limit < 0)
            throw new EOFException();
    }
}
