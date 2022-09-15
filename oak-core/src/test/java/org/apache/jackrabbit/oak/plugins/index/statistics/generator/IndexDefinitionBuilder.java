package org.apache.jackrabbit.oak.plugins.index.statistics.generator;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class IndexDefinitionBuilder {
	private final NodeBuilder builder = EMPTY_NODE.builder();
	private final Map<String, IndexRule> rules = Maps.newHashMap();
	private final Map<String, AggregateRule> aggRules = Maps.newHashMap();
	private final NodeBuilder indexRule;
	private NodeBuilder aggregateBuilder;
	private final static String INDEX_RULES = "indexRules";
	private final static String EVALUATE_PATH_RESTRICTION = "evaluatePathRestrictions";
	private final static String PROPERTIES = "properties";
	private final static String INDEX_NODE_NAME = "indexNodeName";
	private final static String PROP_USE_IN_EXCERPT = "useInExcerpt";

	private final static String PROP_NODE_SCOPE_INDEX = "nodeScopeIndex";

	private final static String PROP_PROPERTY_INDEX = "propertyIndex";

	private final static String PROP_ANALYZED = "analyzed";
	public static final String PROP_ORDERED = "ordered";
	public static final String PROP_TYPE = "type";
	public static final String PROP_NULL_CHECK_ENABLED = "nullCheckEnabled";
	public static final String PROP_NOT_NULL_CHECK_ENABLED = "notNullCheckEnabled";
	public static final String AGGREGATES = "aggregates";
	public static final String AGG_PATH = "path";
	public static final String AGG_RELATIVE_NODE = "relativeNode";

	public IndexDefinitionBuilder() {
//		builder.setProperty(LuceneIndexConstants.COMPAT_MODE, 2);
		builder.setProperty("async", "async");
		builder.setProperty("type", "lucene");
		builder.setProperty(JCR_PRIMARYTYPE, "oak:QueryIndexDefinition", Type.NAME);
		indexRule = createChild(builder, INDEX_RULES);
	}

	public IndexDefinitionBuilder evaluatePathRestrictions() {
		builder.setProperty(EVALUATE_PATH_RESTRICTION, true);
		return this;
	}

	public IndexDefinitionBuilder includedPaths(String... paths) {
		builder.setProperty(createProperty(PathFilter.PROP_INCLUDED_PATHS, Arrays.asList(paths), Type.STRINGS));
		return this;
	}

	public IndexDefinitionBuilder excludedPaths(String... paths) {
		builder.setProperty(createProperty(PathFilter.PROP_EXCLUDED_PATHS, Arrays.asList(paths), Type.STRINGS));
		return this;
	}

	public NodeState build() {
		return builder.getNodeState();
	}

	// ~--------------------------------------< IndexRule >

	public IndexRule indexRule(String type) {
		IndexRule rule = rules.get(type);
		if (rule == null) {
			rule = new IndexRule(createChild(indexRule, type), type);
			rules.put(type, rule);
		}
		return rule;
	}

	public static class IndexRule {
		private final NodeBuilder builder;
		private final NodeBuilder propertiesBuilder;
		private final String ruleName;
		private final Map<String, PropertyRule> props = Maps.newHashMap();
		private final Set<String> propNodeNames = Sets.newHashSet();

		private IndexRule(NodeBuilder builder, String type) {
			this.builder = builder;
			this.propertiesBuilder = createChild(builder, PROPERTIES);
			this.ruleName = type;
		}

		public IndexRule indexNodeName() {
			builder.setProperty(INDEX_NODE_NAME, true);
			return this;
		}

		public PropertyRule property(String name) {
			PropertyRule propRule = props.get(name);
			if (propRule == null) {
				propRule = new PropertyRule(this, createChild(propertiesBuilder, createPropNodeName(name)), name);
				props.put(name, propRule);
			}
			return propRule;
		}

		private String createPropNodeName(String name) {
			name = getSafePropName(name);
			if (name.isEmpty()) {
				name = "prop";
			}
			if (propNodeNames.contains(name)) {
				name = name + "_" + propNodeNames.size();
			}
			propNodeNames.add(name);
			return name;
		}

		public String getRuleName() {
			return ruleName;
		}
	}

	public static class PropertyRule {
		private final IndexRule indexRule;
		private final NodeBuilder builder;

		private PropertyRule(IndexRule indexRule, NodeBuilder builder, String name) {
			this.indexRule = indexRule;
			this.builder = builder;
			builder.setProperty(PROPERTIES, name);
		}

		public PropertyRule useInExcerpt() {
			builder.setProperty(PROP_USE_IN_EXCERPT, true);
			return this;
		}

		public PropertyRule analyzed() {
			builder.setProperty(PROP_ANALYZED, true);
			return this;
		}

		public PropertyRule nodeScopeIndex() {
			builder.setProperty(PROP_NODE_SCOPE_INDEX, true);
			return this;
		}

		public PropertyRule ordered() {
			builder.setProperty(PROP_ORDERED, true);
			return this;
		}

		public PropertyRule ordered(String type) {
			// This would throw an IAE if type is invalid
			int typeValue = PropertyType.valueFromName(type);
			builder.setProperty(PROP_ORDERED, true);
			builder.setProperty(PROP_TYPE, type);
			return this;
		}

		public PropertyRule propertyIndex() {
			builder.setProperty(PROP_PROPERTY_INDEX, true);
			return this;
		}

		public PropertyRule nullCheckEnabled() {
			builder.setProperty(PROP_NULL_CHECK_ENABLED, true);
			return this;
		}

		public PropertyRule notNullCheckEnabled() {
			builder.setProperty(PROP_NOT_NULL_CHECK_ENABLED, true);
			return this;
		}

		public IndexRule enclosingRule() {
			return indexRule;
		}
	}

	// ~--------------------------------------< Aggregates >

	public AggregateRule aggregateRule(String type) {
		if (aggregateBuilder == null) {
			aggregateBuilder = createChild(builder, AGGREGATES);
		}
		AggregateRule rule = aggRules.get(type);
		if (rule == null) {
			rule = new AggregateRule(createChild(aggregateBuilder, type));
			aggRules.put(type, rule);
		}
		return rule;
	}

	public AggregateRule aggregateRule(String primaryType, String... includes) {
		AggregateRule rule = aggregateRule(primaryType);
		for (String include : includes) {
			rule.include(include);
		}
		return rule;
	}

	public static class AggregateRule {
		private final NodeBuilder builder;
		private final Map<String, Include> includes = Maps.newHashMap();

		private AggregateRule(NodeBuilder builder) {
			this.builder = builder;
		}

		public Include include(String includePath) {
			Include include = includes.get(includePath);
			if (include == null) {
				include = new Include(createChild(builder, "include" + includes.size()));
				includes.put(includePath, include);
			}
			include.path(includePath);
			return include;
		}

		public static class Include {
			private final NodeBuilder builder;

			private Include(NodeBuilder builder) {
				this.builder = builder;
			}

			public Include path(String includePath) {
				builder.setProperty(AGG_PATH, includePath);
				return this;
			}

			public Include relativeNode() {
				builder.setProperty(AGG_RELATIVE_NODE, true);
				return this;
			}
		}
	}

	private static NodeBuilder createChild(NodeBuilder builder, String name) {
		NodeBuilder result = builder.child(name);
		result.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME);
		return result;
	}

	static String getSafePropName(String relativePropName) {
		String propName = PathUtils.getName(relativePropName);
		int indexOfColon = propName.indexOf(':');
		if (indexOfColon > 0) {
			propName = propName.substring(indexOfColon + 1);
		}

		// Just keep ascii chars
		propName = propName.replaceAll("\\W", "");
		return propName;
	}
}
