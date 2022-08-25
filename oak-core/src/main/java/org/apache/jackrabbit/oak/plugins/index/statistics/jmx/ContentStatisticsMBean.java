package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

import java.util.List;

import org.apache.jackrabbit.oak.api.jmx.Description;
import org.apache.jackrabbit.oak.api.jmx.Name;

public interface ContentStatisticsMBean {
	String TYPE = "ContentStatistics";

	/**
	 * Get the estimated statistics of a single property.
	 * 
	 * @param name - the property for which to get statistics from
	 * @return
	 */
	@Description("Returns an estimation of the cardinality and count of the given property name")
	EstimationResult getSinglePropertyEstimation(@Description("the property name") @Name("name") String name);

	/**
	 * Get the estimated number of statistics for all indexed properties.
	 * 
	 * @return
	 */
	@Description("Get the estimated number of all properties and their cardinality.")
	List<EstimationResult> getAllPropertiesEstimation();

	@Description("Get the estimated number of entries of the given property name")
	long getEstimatedPropertyCount(@Description("the property name") @Name("name") String name);

	List<String> getIndexedPropertyNames();

//	long get
	// method for single property
	// method for all properties sorted by frequency
	// method for getting properties ordered by name
	// e.g createdBy and created are shown alphabetically
	//

	// getPropertyByName
	// getPropertyByCount

}
