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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.Map;

public class StatisticsEditor implements Editor {

    public static final String DATA_NODE_NAME = "index";

    public static final int DEFAULT_RESOLUTION = 1000;

    private final CountMinSketch propertyNameCMS;
    private Map<String, PropertyStatistics> propertyStatistics;
    private final StatisticsRoot root;
    private final StatisticsEditor parent;
    private final String name;
    private SipHash hash;
    private int recursionLevel;

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
    	recursionLevel++;
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
//        root.callback.indexUpdate();
        recursionLevel--;
        if (recursionLevel > 0) {
        	return;
        }
        
        NodeBuilder data = root.definition.child(DATA_NODE_NAME); 
        NodeBuilder builder = data.child("properties");
        for (Map.Entry<String, PropertyStatistics> entry : propertyStatistics.entrySet()) {
        	NodeBuilder c = builder.child(entry.getKey());
        	c.setProperty("count", entry.getValue().getCount());
        	// TODO: consider using HyperLogLog4TailCut64 so that we only store a long rather than array. 
        	c.setProperty("uniqueHLL", entry.getValue().getHll().getCounts());
        }
        propertyStatistics.clear();
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
            Type<?> t = after.getType();
            if (after.isArray()) {
                Type<?> base = t.getBaseType();
                int count = after.count();
                for (int i = 0; i < count; i++) {
                	updatePropertyStatistics(propertyName, after.getValue(base, i));
                }
            } else {
                updatePropertyStatistics(propertyName, after.getValue(t));
            }
        }
    }		
    
    private void updatePropertyStatistics(String propertyName, Object val) {
        PropertyStatistics ps = propertyStatistics.get(propertyName);
        if (ps == null) {
        	// TODO: load data from previous index
        	NodeBuilder data = root.definition.child(DATA_NODE_NAME);
        	@Nullable PropertyState count = data.child("properties").child(propertyName).getProperty("count");
        	System.out.println(propertyName + ": " + count);
        	if (count == null) {
        		ps = new PropertyStatistics(propertyName, 0, new HyperLogLog(64)); 
        	} else {        		
        		
        		long c = count.getValue(Type.LONG);
        		
        		PropertyState hps = data.child("properties").child(propertyName).getProperty("uniqueHLL");
        		// TODO: Can we use Type.REFERENCE here? Then we shouldn't only store the array 
        		// but the whole HLL object. 
				// Iterable<Long> hllArray = hps.getValue(Type.LONGS);
        		ps = new PropertyStatistics(propertyName, c, new HyperLogLog(64));
        	}
        	propertyStatistics.put(propertyName, ps);
        	long hash64 = Hash.hash64((val.hashCode()));
        	ps.updateHll(hash64);
        } 
        ps.inc(1);
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

        return this;
    }

    @Override
    @Nullable
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {

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
