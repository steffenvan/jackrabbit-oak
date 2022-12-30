package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.plugins.index.statistics.IndexUtil;
import org.apache.jackrabbit.oak.plugins.index.statistics.PropertyStatistics;
import org.apache.jackrabbit.oak.plugins.index.statistics.StatisticsEditor;
import org.apache.jackrabbit.oak.plugins.index.statistics.StatisticsEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.statistics.StatisticsIndexHelper;
import org.apache.jackrabbit.oak.plugins.index.statistics.TopKValues;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.plugins.index.statistics.IndexUtil.getIndexRoot;
import static org.apache.jackrabbit.oak.plugins.index.statistics.IndexUtil.getPropertiesOf;
import static org.apache.jackrabbit.oak.plugins.index.statistics.StatisticsIndexHelper.getNodeFromIndexRoot;

public class ContentStatistics extends AnnotatedStandardMBean implements ContentStatisticsMBean {

    public static final Logger CS_LOG = LoggerFactory.getLogger(
            ContentStatistics.class);
    private final NodeStore store;

    public ContentStatistics(NodeStore store) {
        super(ContentStatisticsMBean.class);
        this.store = store;
    }

    @Override
    public Optional<PropertyStatistics> getSinglePropertyEstimation(
            String name) {
        NodeState dataNode = StatisticsIndexHelper.getNodeFromIndexRoot(
                getIndexRoot(store));

        return PropertyStatistics.fromPropertyNode(name, dataNode);
    }

    @Override
    public List<PropertyStatistics> getAllPropertyStatistics() {
        // oak:index/statistics/index/properties
        NodeState propNode = getNodeFromIndexRoot(getIndexRoot(store));

        if (!propNode.exists()) {
            return Collections.emptyList();
        }

        List<PropertyStatistics> propStats = new ArrayList<>();

        for (ChildNodeEntry child : propNode.getChildNodeEntries()) {
            Optional<PropertyStatistics> ps = PropertyStatistics.fromPropertyNode(
                    child.getName(), propNode);
            ps.ifPresent(propStats::add);

        }
        return propStats.stream()
                        .sorted(Comparator.comparing(
                                                  PropertyStatistics::getCount)
                                          .reversed()
                                          .thenComparingLong(
                                                  PropertyStatistics::getUniqueCount))
                        .collect(Collectors.toList());
    }

    @Override
    public Set<String> getIndexedPropertyNames() {
        NodeState indexNode = getIndexRoot(store);
        Set<String> indexedPropertyNames = new TreeSet<>();
        for (ChildNodeEntry entry : indexNode.getChildNodeEntries()) {
            NodeState child = entry.getNodeState();
            // TODO: this will take 2 * n iterations. Should we pass the set
            //  in as a
            // reference and update it directly to reduce it to n iterations?
            indexedPropertyNames.addAll(getPropertiesOf(child));
        }

        return indexedPropertyNames;
    }

    @Override
    public Set<String> getPropertyNamesForSingleIndex(String name) {
        return IndexUtil.getPropertiesOf(
                IndexUtil.getIndexedNodeFromName(name, store));
    }

    @Override
    public List<TopKValues.ValueCountPair> getTopKValuesForProperty(String name,
                                                                    int k) {
        return StatisticsIndexHelper.getTopValues(name, getIndexRoot(store));
    }

    private List<TopKValues.ProportionInfo> getProportionInfoForSingleProperty(
            String propertyName) {

        Optional<PropertyStatistics> ps = PropertyStatistics.fromPropertyNode(
                propertyName, StatisticsIndexHelper.getNodeFromIndexRoot(
                        getIndexRoot(store)));
        if (!ps.isPresent()) {
            return Collections.emptyList();
        }

        PropertyStatistics propStats = ps.get();
        List<TopKValues.ValueCountPair> topK = propStats.getTopKValuesDescending();
        List<TopKValues.ProportionInfo> proportionInfo = new ArrayList<>();

        long totalCount = StatisticsIndexHelper.getCount(propertyName,
                                                         getIndexRoot(store));
        for (TopKValues.ValueCountPair pi : topK) {
            TopKValues.ProportionInfo dpi = new TopKValues.ProportionInfo(
                    pi.getValue(), pi.getCount(), totalCount);
            proportionInfo.add(dpi);
        }

        long topKTotalCount = proportionInfo.stream()
                                            .mapToLong(
                                                    TopKValues.ProportionInfo::getCount)
                                            .sum();
        TopKValues.ProportionInfo totalInfo = new TopKValues.ProportionInfo(
                "TopKCount", topKTotalCount, totalCount);
        proportionInfo.add(totalInfo);
        return proportionInfo;
    }

    @Override
    public List<TopKValues.ProportionInfo> getValueProportionInfoForSingleProperty(
            String propertyName) {
        return getProportionInfoForSingleProperty(propertyName);
    }

    @Override
    public List<List<TopKValues.ProportionInfo>> getProportionInfoForIndexedProperties() {
        NodeState indexNode = getIndexRoot(store);
        List<List<TopKValues.ProportionInfo>> propInfo = new ArrayList<>();
        Iterable<? extends ChildNodeEntry> propertyNode = indexNode.getChildNode(
                                                                           StatisticsEditorProvider.TYPE)
                                                                   .getChildNode(
                                                                           "index")
                                                                   .getChildNode(
                                                                           StatisticsEditor.PROPERTIES)
                                                                   .getChildNodeEntries();
        for (ChildNodeEntry child : propertyNode) {
            propInfo.add(getProportionInfoForSingleProperty(child.getName()));
        }
        return propInfo;
    }
}
