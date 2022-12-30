package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

import org.apache.jackrabbit.oak.api.jmx.Description;
import org.apache.jackrabbit.oak.api.jmx.Name;
import org.apache.jackrabbit.oak.plugins.index.statistics.PropertyStatistics;
import org.apache.jackrabbit.oak.plugins.index.statistics.TopKValues;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface ContentStatisticsMBean {
    String TYPE = "ContentStatistics";

    /**
     * Get the statistics information for a single property. This information
     * includes the CountMinSketch (count), HyperLogLog (cardinality), top K
     * values and more.
     *
     * @param propertyName - the string representation of the property
     * @return a single EstimationResult object that wraps the
     */
    @Description(
            "Returns the statistical information of a single property. " + "This includes the estimated count, cardinality, average " + "value " + "length, top K most frequent values and more.")
    Optional<PropertyStatistics> getSinglePropertyStatistics(
            @Description("The property name (e.g jcr:primaryType)") @Name(
                    "name") String propertyName);

    /**
     * Get the estimated number of statistics for all indexed properties.
     *
     * @return each indexed property and their corresponding statistics
     * information as valid JSON.
     */
    @Description(
            "Returns the statistical information of all indexed properties in " + "the repository. This includes the estimated count, " + "cardinality, " + "average value length, top K most " + "frequent values and more.")
    List<PropertyStatistics> getAllPropertyStatistics();

    /**
     * This is added in addition to the list of EstimationResults, as we might
     * be interested in just what properties are indexed.
     *
     * @return the set of indexed property names in the repository.
     */
    @Description(
            "Returns the all the indexed property names of the " + "repository.")
    Set<String> getIndexedPropertyNames();

    /**
     * This function is used to conveniently fetch the names of the properties
     * that are stored under an index. Because, we typically need to traverse a
     * few nodes down from the indexNode to fetch the properties, do some
     * filtering, parsing and more.
     *
     * @param indexName - the string value of the index. Could be "socialLucene"
     *                  or "counter"
     * @return the set of property names as strings that are stored under
     */
    @Description(
            "Returns the properties of the specified index (where " + "indexName could e.g be socialLucene)")
    Set<String> getPropertiesOfSingleIndex(
            @Description("The index name") String indexName);

    /**
     * Returns the top k values for a specific property. Note that the length of
     * the output will be: min(k, TopKValues.k). That means if the provided k is
     * larger than what is indexed (e.g. we have indexed top 5, but the client
     * passes in 10), it will only print out the top 5 values. And the same for
     * the other way around.
     *
     * @param propertyName - the property name we want the top k values for
     * @param k            - the top k values of the specified property.
     * @return a list of ValueCountPair (valueName, count) of the top k most
     * frequent values.
     */
    @Description(
            "Returns the top K values for the specified property, where " + "k = min(k, 5) (as k = 5 is the repository's default " + "value).")
    List<TopKValues.ValueCountPair> getTopKValuesForProperty(
            String propertyName, int k);

    @Description("Returns all the top k values for the indexed properties")
    List<List<TopKValues.ProportionInfo>> getProportionInfoForIndexedProperties();

    /**
     * Returns the top K values and what fraction they make out of the estimated
     * total counts of that property.
     *
     * @param propertyName - the property for which we want the top K values
     *                     for
     * @return a list of the top k values and what fraction they make out of the
     * estimated count of that property.
     */
    @Description(
            "Returns the percentage each of the top K values make out " + "of" + " all the values of the specified property and what " + "percentage " + "all the top K values make out of the property.")
    List<TopKValues.ProportionInfo> getValueProportionInfoForSingleProperty(
            String propertyName);
}
