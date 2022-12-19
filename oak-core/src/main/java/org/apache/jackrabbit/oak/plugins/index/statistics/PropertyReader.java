package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;

/**
 * Represents a group of utility functions that makes it easier to read the
 * values from a PropertyState.
 */
public class PropertyReader {
    public static Long getLongOrZero(PropertyState ps) {
        return ps == null ? Long.valueOf(0) : ps.getValue(Type.LONG);
    }

    public static String getStringOrEmpty(PropertyState ps) {
        return ps == null ? "" : ps.getValue(Type.STRING);
    }
}
