package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.counter.SipHash;
import org.apache.jackrabbit.oak.plugins.index.counter.jmx.NodeCounter;
import org.apache.jackrabbit.oak.plugins.index.property.Multiplexers;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

public class StatisticsEditor implements Editor {

    public static final String DATA_NODE_NAME = "index";

    // the property that is used with the "new" (hash of the path based) method
    public static final String COUNT_HASH_PROPERTY_NAME = "cnt";

    public static final int DEFAULT_RESOLUTION = 1000;

    private final StatisticsRoot root;
    private final StatisticsEditor parent;
    private final String name;
    private final MountInfoProvider mountInfoProvider;
    private final Map<Mount, Integer> countOffsets;
    private final Mount currentMount;
    private final boolean mountCanChange;
    private SipHash hash;


    StatisticsEditor(StatisticsRoot root, MountInfoProvider mountInfoProvider) {
        this.root = root;
        this.name = "/";
        this.parent = null;
        this.mountInfoProvider = mountInfoProvider;
        this.currentMount = mountInfoProvider.getDefaultMount();
        this.mountCanChange = true;
        this.countOffsets = new HashMap<>();
    }

    private StatisticsEditor(StatisticsRoot root, StatisticsEditor parent, String name, SipHash hash, MountInfoProvider mountInfoProvider) {
        this.parent = parent;
        this.root = root;
        this.name = name;
        this.hash = hash;
        this.mountInfoProvider = mountInfoProvider;
        this.countOffsets = new HashMap<>();
        if (parent.mountCanChange) {
            String path = getPath();
            this.currentMount = mountInfoProvider.getMountByPath(path);
            this.mountCanChange = currentMount.isDefault() && supportMounts(path);
        } else {
            this.currentMount = this.parent.currentMount;
            this.mountCanChange = false;
        }
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
        if (countOffsets.isEmpty()) {
            return;
        }
        root.callback.indexUpdate();
        for (Map.Entry<Mount, Integer> e : countOffsets.entrySet()) {
            Mount mount = e.getKey();
            if (mount.isReadOnly()) {
                continue;
            }
            NodeBuilder builder = getBuilder(mount);
            int countOffset = e.getValue();

            PropertyState p = builder.getProperty(COUNT_HASH_PROPERTY_NAME);
            long count = p == null ? 0 : p.getValue(Type.LONG);
            count += countOffset;
            if (count <= 0) {
                if (builder.getChildNodeCount(1) >= 0) {
                    builder.removeProperty(COUNT_HASH_PROPERTY_NAME);
                } else {
                    builder.remove();
                }
            } else {
                builder.setProperty(COUNT_HASH_PROPERTY_NAME, count);
            }
        }
    }

    private NodeBuilder getBuilder(Mount mount) {
        if (parent == null) {
            return root.definition.child(Multiplexers.getNodeForMount(mount, DATA_NODE_NAME));
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
        // nothing to do
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        // nothing to do
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
        return getChildIndexEditor(name, null);
    }

    @Override
    @Nullable
    public Editor childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        if (NodeCounter.COUNT_HASH) {
            SipHash h = new SipHash(getHash(), name.hashCode());
            // with bitMask=1024: with a probability of 1:1024,
            if ((h.hashCode() & root.bitMask) == 0) {
                // add 1024
                count(root.bitMask + 1, currentMount);
            }
            return getChildIndexEditor(name, h);
        }
        count(1, currentMount);
        return getChildIndexEditor(name, null);
    }

    @Override
    @Nullable
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        if (NodeCounter.COUNT_HASH) {
            SipHash h = new SipHash(getHash(), name.hashCode());
            // with bitMask=1024: with a probability of 1:1024,
            if ((h.hashCode() & root.bitMask) == 0) {
                // subtract 1024
                count(-(root.bitMask + 1), currentMount);
            }
            return getChildIndexEditor(name, h);
        }
        count(-1, currentMount);
        return getChildIndexEditor(name, null);
    }

    private void count(int offset, Mount mount) {
        countOffsets.compute(mount, (m, v) -> v == null ? offset : v + offset);
        if (parent != null) {
            parent.count(offset, mount);
        }
    }

    private Editor getChildIndexEditor(String name, SipHash hash) {
        return new StatisticsEditor(root, this, name, hash, mountInfoProvider);
    }

    private boolean supportMounts(String path) {
        return mountInfoProvider
                .getNonDefaultMounts()
                .stream()
                .anyMatch(m -> m.isSupportFragmentUnder(path) || m.isUnder(path));
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
