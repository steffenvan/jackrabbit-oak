package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

import java.util.List;

import org.apache.jackrabbit.oak.api.jmx.Description;
import org.apache.jackrabbit.oak.api.jmx.Name;

public interface StatisticsMBean {
	String TYPE = "Statistics";

	/**
	 * Get the estimated statistics of a single property.
	 * 
	 * @param name - the property for which to get statistics from
	 * @return
	 */
	@Description("Get the estimated number of properties of the specified property")
	EstimationResult getSinglePropertyEstimation(@Description("the path") @Name("name") String name);

	/**
	 * Get the estimated number of statistics for all indexed properties.
	 * 
	 * @return
	 */
	@Description("Get the estimated number of all indexed properties")
	List<EstimationResult> getAllPropertiesEstimation();

	@Description("Get the estimated number property entries for this specific")
	long getEstimatedPropertyCount(String name);

//	long get
	// method for single property
	// method for all properties sorted by frequency
	// method for getting properties ordered by name
	// e.g createdBy and created are shown alphabetically
	//

	// getPropertyByName
	// getPropertyByCount

}
