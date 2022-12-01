package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class JsonArrayParser {

    private static List<String> read(String json, int type) {
        List<String> result = new ArrayList<>();
        JsopTokenizer tokenizer = new JsopTokenizer(json);

        tokenizer.read('[');
        if (!tokenizer.matches(']')) {
            do {
                result.add(tokenizer.read(type));
            } while (tokenizer.matches(','));
            tokenizer.read(']');
        }

        tokenizer.read(JsopTokenizer.END);

        return result;
    }

    public static List<String> readString(String json) {
        return read(json, JsopReader.STRING);
    }

    public static List<Long> readNumbers(String json) {
        return read(json, JsopReader.NUMBER).stream()
                                            .map(Long::parseLong)
                                            .collect(Collectors.toList());
    }
}
