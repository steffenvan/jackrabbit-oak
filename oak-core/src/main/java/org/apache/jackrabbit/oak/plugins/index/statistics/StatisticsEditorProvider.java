/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.statistics;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.counter.jmx.NodeCounter;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.service.component.annotations.Component;

@Component(service = IndexEditorProvider.class)
public class StatisticsEditorProvider implements IndexEditorProvider {

	public static final String TYPE = "statistics";

	public static final String RESOLUTION = "resolution";

	public static final String SEED = "seed";

    public static final String COMMON_PROPERTY_THRESHOLD = "commonPropertyThreshold";
    public static final String PROPERTY_CMS_ROWS = "propertyCMSRows";
    public static final String PROPERTY_CMS_COLS = "propertyCMSCols";
    public static final String VALUE_CMS_ROWS = "valueCMSRows";
    public static final String VALUE_CMS_COLS = "valueCMSCols";

    @Override
    @Nullable
    public Editor getIndexEditor(@NotNull String type,
                                 @NotNull NodeBuilder definition, @NotNull NodeState root,
                                 @NotNull IndexUpdateCallback callback) throws CommitFailedException {
        if (!TYPE.equals(type)) {
            return null;
        }
        int resolution;
        PropertyState s = definition.getProperty(RESOLUTION);
        if (s == null) {
            resolution = StatisticsEditor.DEFAULT_RESOLUTION;
        } else {
            resolution = s.getValue(Type.LONG).intValue();
        }
        long seed;
        s = definition.getProperty(SEED);
        if (s != null) {
            seed = s.getValue(Type.LONG).intValue();
        } else {
            seed = 0;
            if (NodeCounter.COUNT_HASH) {
                // create a random number (that way we can also check if this feature is enabled)
                seed = UUID.randomUUID().getMostSignificantBits();
                definition.setProperty(SEED, seed);
            }
        }
        int commonPropertyThreshold;
        s = definition.getProperty(COMMON_PROPERTY_THRESHOLD);
        if (s == null) {
            commonPropertyThreshold = 10;
        } else {
            commonPropertyThreshold = s.getValue(Type.LONG).intValue();
        }

        PropertyState rows = definition.getProperty(PROPERTY_CMS_ROWS);
        PropertyState cols = definition.getProperty(PROPERTY_CMS_COLS);
        int propertyCMSRows = 7;
        int propertyCMSCols = 64;
        if (rows != null && cols != null) {
            propertyCMSRows = rows.getValue(Type.LONG).intValue();
            propertyCMSCols = cols.getValue(Type.LONG).intValue();
        }

        rows = definition.getProperty(VALUE_CMS_ROWS);
        cols = definition.getProperty(VALUE_CMS_COLS);
        int valueCMSRows = 5;
        int valueCMSCols = 32;
        if (rows != null && cols != null) {
            propertyCMSRows = rows.getValue(Type.LONG).intValue();
            propertyCMSCols = cols.getValue(Type.LONG).intValue();
        }

        StatisticsEditor.StatisticsRoot rootData = new StatisticsEditor.StatisticsRoot(
                resolution, seed, definition, root, callback, commonPropertyThreshold);
        CountMinSketch cms = new CountMinSketch(propertyCMSRows, propertyCMSCols);
        Map<String, PropertyStatistics> propertyStatistics = new HashMap<>();

        return new StatisticsEditor(rootData, cms, propertyStatistics, valueCMSRows, valueCMSCols);
    }
}
