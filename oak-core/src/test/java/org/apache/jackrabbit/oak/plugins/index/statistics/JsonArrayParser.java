package org.apache.jackrabbit.oak.plugins.index.statistics.state.export;

import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;

import java.util.ArrayList;
import java.util.List;

public class JsonArrayParser {

    public static List<String> read(String json, int type) {
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
}
