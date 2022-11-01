package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;

public class PropertyReader {
    public static Long getLongOrZero(PropertyState ps) {
        return ps == null ? Long.valueOf(0) : ps.getValue(Type.LONG);
    }

    public static String getStringOrEmpty(PropertyState ps) {
        return ps == null ? "" : ps.getValue(Type.STRING);
    }
}
