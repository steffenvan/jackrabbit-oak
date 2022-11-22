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
package org.apache.jackrabbit.oak.index.statistics;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.felix.inventory.Format;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.index.IndexOptions;
import org.apache.jackrabbit.oak.plugins.index.CorruptIndexHandler;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.NodeTraversalCallback;
import org.apache.jackrabbit.oak.plugins.index.statistics.StatisticsEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.statistics.jmx.ContentStatistics;
import org.apache.jackrabbit.oak.plugins.index.statistics.jmx.StatisticsDefinitionPrinter;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.run.commons.LoggingInitializer;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import joptsimple.OptionParser;

public class ContentStatisticsCommand implements Command {

    private static final Logger LOG = LoggerFactory.getLogger(ContentStatisticsCommand.class);

    public static final String NAME = "content-statistics";
    private final static String SUMMARY = "Build the content statistics for a repository";
    private static final String REINDEX_LANE = "offline-reindex-async";

    private Options opts;

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        opts = new Options();
        opts.setCommandName(NAME);
        opts.setSummary(SUMMARY);
        opts.setConnectionString(CommonOptions.DEFAULT_CONNECTION_STRING);
        opts.registerOptionsFactory(IndexOptions.FACTORY);
        opts.parseAndConfigure(parser, args);
        try {
            try (Closer closer = Closer.create()) {
                NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts);
                closer.register(fixture);
                execute(fixture);
            }
        } catch (Throwable e) {
            LOG.error("Error occurred while performing index tasks", e);
            e.printStackTrace(System.err);
            throw e;
        } finally {
            LoggingInitializer.shutdownLogging();
        }
    }

    private void execute(NodeStoreFixture fixture) throws CommitFailedException, IOException {
        MemoryNodeStore output = new MemoryNodeStore();
        NodeBuilder builder = output.getRoot().builder();
        NodeBuilder statisticsIndex = builder.
            child(IndexConstants.INDEX_DEFINITIONS_NAME).
            child(ContentStatistics.STATISTICS_INDEX_NAME);
        statisticsIndex.setProperty("type", "statistics");
        statisticsIndex.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                IndexConstants.INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        statisticsIndex.setProperty("async", REINDEX_LANE);

        StatisticsEditorProvider statistics = new StatisticsEditorProvider();
        Callback callback = new Callback();
        IndexUpdate indexUpdate = new IndexUpdate(
                statistics,
                REINDEX_LANE,
                output.getRoot(),
                builder,
                callback,
                callback,
                CommitInfo.EMPTY,
                CorruptIndexHandler.NOOP
        );

        NodeState before = output.getRoot();
        NodeState after = fixture.getStore().getRoot();

        EditorDiff.process(VisibleEditor.wrap(indexUpdate), before, after);
        output.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        StatisticsDefinitionPrinter printer = new StatisticsDefinitionPrinter(output);
        // TODO write to a file
        String name = "statistics";
        String extension = ".json";
        int num = 0;
        String fName = name + num + extension;
        File f = new File(fName);
        while (f.exists()) {
            num++;
            fName = name + num + extension;
            f = new File(fName);
        }

        PrintWriter out = new PrintWriter(new FileWriter(fName));
        printer.print(out, Format.JSON, false);
        out.close();
    }

    static class Callback implements IndexUpdateCallback, NodeTraversalCallback {

        @Override
        public void traversedNode(PathSource arg0) throws CommitFailedException {
            // ignore
        }

        @Override
        public void indexUpdate() throws CommitFailedException {
            // ignore
        }

    }


}
