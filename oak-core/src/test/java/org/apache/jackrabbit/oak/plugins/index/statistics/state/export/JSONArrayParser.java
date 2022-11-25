package org.apache.jackrabbit.oak.plugins.index.statistics.state.export;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JSONArrayParser {
    private final String text;
    private int ix;

    public JSONArrayParser(String text) {
        this.text = text;
    }

    public static void main(String[] args) {
        String text = " [ \"hello\" , \"world\" ] ";
        String text2 = "[\"/libs/dam/templates/marketingcloud/thumbnail.png\", \"/libs/cq/contentinsight/templates/brightedge-cloudservice-config/thumbnail.png\", \"/libs/dam/templates/youtube/thumbnail.png\", \"/libs/cq/analytics/templates/testandtarget/thumbnail.png\", \"/libs/cq/analytics/templates/sitecatalyst/thumbnail.png\"]";
        JSONArrayParser JSONArrayParser = new JSONArrayParser(text2);
        System.out.println(Arrays.toString(JSONArrayParser.parse()));
    }

    private void consumeWS() {
        while (ix < text.length() && Character.isWhitespace(text.charAt(ix))) {
            ix++;
        }
    }

    public String[] parse() {
        consumeWS();
        if (ix == text.length() || text.charAt(ix) != '[') {
            System.out.println("missing value at start of expression");
            return null;
        }
        ix++;
        List<String> strings = new ArrayList<>();
        consumeWS();
        while (ix < text.length() && text.charAt(ix) != ']') {
            if (text.charAt(ix) != '"') {
                System.out.println("missing string in list, unexpected character " + text.charAt(ix));
                return null;
            }
            ix++;
            StringBuilder sb = new StringBuilder();
            while (ix < text.length() && text.charAt(ix) != '"') {
                if (ix != '\\') {
                    sb.append(text.charAt(ix));
                    ix++;
                }
                else {
                    sb.append(text.charAt(ix+1));
                    ix += 2;
                }
            }
            if (ix == text.length()) {
                System.out.println("unexpected end of expression before closing string: " + sb);
                return null;
            }
            ix++;

            strings.add(sb.toString());
            consumeWS();
            if (ix == text.length()) {
                System.out.println("unexpected end of expression before closing list");
                return null;
            }
            if (text.charAt(ix) == ',') {
                ix++;
                consumeWS();
            }
        }
        if (text.charAt(ix) != ']') {
            System.out.println("unexpected end of expression awaiting strings in list");
            return null;
        }

        return strings.toArray(new String[0]);
    }
}
