package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.counter.SipHash;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

public class StatisticsEditor implements Editor {

    public static final String DATA_NODE_NAME = "index";
    public static final String CMS_NODE_NAME = "cms";
    public static final String HLL_NAME = "hll";

    public static final String UNIQUE_PROPERTY_COUNT_NAME = "uniqueCount";


    public static final int DEFAULT_RESOLUTION = 1000;

    private final CountMinSketch propertyNameCMS;
    private Map<String, PropertyStatistics> propertyStatistics;
    private final StatisticsRoot root;
    private final StatisticsEditor parent;
    private final String name;
    private SipHash hash;

    StatisticsEditor(StatisticsRoot root, CountMinSketch propertyNameCMS, Map<String, PropertyStatistics> propertyStatistics) {
        this.root = root;
        this.name = "/";
        this.parent = null;
        this.propertyNameCMS = propertyNameCMS;
        this.propertyStatistics = propertyStatistics;
    }

    private StatisticsEditor(StatisticsRoot root, StatisticsEditor parent, String name, SipHash hash, CountMinSketch propertyNameCMS, Map<String, PropertyStatistics> propertyStatistics) {
        this.parent = parent;
        this.root = root;
        this.name = name;
        this.hash = hash;
        this.propertyNameCMS = propertyNameCMS;
        this.propertyStatistics = propertyStatistics;
    }

    private SipHash getHash() {
        if (hash != null) {
            return hash;
        }
        SipHash h;
        if (parent == null) {
            h = new SipHash(root.seed);
        } else {
            h = new SipHash(parent.getHash(), name.hashCode());
        }
        this.hash = h;
        return h;
    }

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        // nothing to do
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        root.callback.indexUpdate();
        PropertyState p = after.getProperty(UNIQUE_PROPERTY_COUNT_NAME);
        NodeBuilder builder = root.definition;
        if (p == null) {
            return;
        }
        long currCount = p.getValue(Type.LONG);
        PropertyStatistics stats = propertyStatistics.get(p.getName());
        long count = stats.getCount();
        currCount += count;
        // TODO: can we face the same issue that the count is negative?
        builder.setProperty(UNIQUE_PROPERTY_COUNT_NAME, currCount);
    }

    private NodeBuilder getBuilder(Mount mount) {
        if (parent == null) {
            return root.definition.child(UNIQUE_PROPERTY_COUNT_NAME);
        } else {
            return parent.getBuilder(mount).child(name);
        }
    }

    private String getPath() {
        if (parent == null) {
            return name;
        } else {
            return PathUtils.concat(parent.getPath(), name);
        }
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        propertyHasNewValue(after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        propertyHasNewValue(after);
    }

    private void propertyHasNewValue(PropertyState after) {
        String propertyName = after.getName();
        int propertyNameHash = propertyName.hashCode();
        long properHash = Hash.hash64(propertyNameHash);
        propertyNameCMS.add(properHash);

        if (propertyNameCMS.propertyNameIsCommon(properHash)) {
//            NodeBuilder builder = root.root.builder();
//            builder.setChildNode(propertyName);
            Type<?> t = after.getType();
            if (after.isArray()) {
//                int count = after.count();
//                for (int i = 0; i<count; i++) {
//                    Object obj = after.getValue(Type.fromTag(t, true), i);
//                    int valueHash = obj.hashCode();
//                    hll.add(Hash.hash64(valueHash));
            } else {
                Object obj = after.getValue(t);
                int valueHash = obj.hashCode();
                PropertyStatistics curr = propertyStatistics.get(propertyName);
                curr.updateStats(Hash.hash64(valueHash));
                curr.inc(1);
            }
        }
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        // nothing to do
    }

    @Override
    @Nullable
    public Editor childNodeChanged(String name, NodeState before, NodeState after)
            throws CommitFailedException {
        return this;
    }

    @Override
    @Nullable
    public Editor childNodeAdded(String name, NodeState after)
            throws CommitFailedException {

//        SipHash h = new SipHash(getHash(), name.hashCode());
//        NodeBuilder builder = after.builder();
//        after.compareAgainstBaseState()

//        // with bitMask=1024: with a probability of 1:1024,
//        if ((h.hashCode() & root.bitMask) == 0) {
//            // add 1024
//            count(root.bitMask + 1);
//        }
//        return getChildIndexEditor(name, h);
//        count(1, currentMount);
//        return getChildIndexEditor(name, null);
        return this;
    }

    @Override
    @Nullable
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
//        if (NodeCounter.COUNT_HASH) {
//            SipHash h = new SipHash(getHash(), name.hashCode());
//            // with bitMask=1024: with a probability of 1:1024,
//            if ((h.hashCode() & root.bitMask) == 0) {
//                // subtract 1024
//                count(-(root.bitMask + 1), currentMount);
//            }
//            return getChildIndexEditor(name, h);
//        }
//        count(-1, currentMount);
//        return getChildIndexEditor(name, null);
        return this;
    }

    private Editor getChildIndexEditor(String name, SipHash hash) {
        return new StatisticsEditor(root, this, name, hash, propertyNameCMS, propertyStatistics);
    }

    public static class StatisticsRoot {
        final int resolution;
        final long seed;
        final int bitMask;
        final NodeBuilder definition;
        final NodeState root;
        final IndexUpdateCallback callback;

        StatisticsRoot(int resolution, long seed, NodeBuilder definition, NodeState root, IndexUpdateCallback callback) {
            this.resolution = resolution;
            this.seed = seed;
            // if resolution is 1000, then the bitMask is 1023 (bits 0..9 set)
            this.bitMask = (Integer.highestOneBit(resolution) * 2) - 1;
            this.definition = definition;
            this.root = root;
            this.callback = callback;
        }
    }
}
