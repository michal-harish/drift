package net.imagini.aim.utils;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class CSVStreamParser {

    private InputStream input;
    private char separator;
    private byte[] buffer = new byte[131070];
    private int position = -1;
    private int limit = -1;

    public CSVStreamParser(InputStream input, char separator) {
        this.input = input;
        this.separator = separator;
    }

    public String nextValue() throws IOException {
        int start = -1;
        int end = -1;
        String result = null;
        try {
            while (true) {
                if (position >= limit - 1) {
                    if (start >= 0 && end >= 0) {
                        String val = new String(buffer, start, end - start + 1);
                        result = result == null ? val : result + val;
                        start = 0;
                        end = -1;
                    }
                    loadBuffer();
                }
                char ch = (char) buffer[++position];
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
        String val;
        if (end >= start) {
            val = new String(buffer, start, end - start + 1);
        } else {
            val = "";
        }
        result = result == null ? val : result + val;
        return result;
    }

    private void loadBuffer() throws IOException {
        limit = input.read(buffer, 0, buffer.length);
        position = -1;
        if (limit < 0)
            throw new EOFException();
    }
}
