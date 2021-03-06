package net.imagini.drift.utils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Tokenizer {

    public static enum Token { 
        WHITESPACE,KEYWORD,OPERATOR,SEPARATOR,NUMBER,STRING
    }

    @SuppressWarnings("serial")
    static final Map<Token,Pattern> matchers = new HashMap<Token,Pattern>() {{
        put(Token.WHITESPACE, Pattern.compile("^((\\s+))"));
        put(Token.KEYWORD, Pattern.compile("^(([A-Za-z_]+[A-Za-z_0-9]*))"));
        put(Token.OPERATOR, Pattern.compile("^(([\\!@\\$%\\^&\\*\\:|<.>/\\?\\-=\\+\\(\\)\\[\\]\\{\\}`~]+))")); // TODO define separately () {} []
        put(Token.SEPARATOR, Pattern.compile("^(([;,]+))"));
        put(Token.NUMBER,  Pattern.compile("^(([0-9]+|[0-9]+\\.[0-9]+))"));
        put(Token.STRING, Pattern.compile("^('(.*?)')")); // TODO fix escape sequence (?:\\"|.)*? OR /'(?:[^'\\]|\\.)*'/
    }};

    public static Queue<String> tokenize(String input, Boolean quoteStrings) {
        Queue<String> result = new LinkedList<String>();
        int i = 0;
        main: while(i<input.length()) {
            String s = input.substring(i);
            for(Entry<Token,Pattern> p: matchers.entrySet()) {
                Matcher m = p.getValue().matcher(s);
                if (m.find()) {
                    i += m.group(1).length();
                    String word = m.group(2);
                    if (!p.getKey().equals(Token.WHITESPACE)) {
                        if (p.getKey().equals(Token.STRING) && quoteStrings) {
                            result.add("'" + word +"'");
                        } else {
                            result.add(word);
                        }
                    }
                    continue main;
                }
            }
            throw new IllegalArgumentException("Invalid query near: " + s);
        }
        return result;
    }
}
