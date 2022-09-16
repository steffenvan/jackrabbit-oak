package org.apache.jackrabbit.oak.plugins.index.statistics.generator;

import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState.squeeze;

import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.plugins.index.statistics.generator.IndexDefinitionBuilder.IndexRule;
import org.apache.jackrabbit.oak.plugins.index.statistics.generator.IndexDefinitionBuilder.PropertyRule;
import org.apache.jackrabbit.oak.query.ExecutionContext;
import org.apache.jackrabbit.oak.query.QueryEngineImpl;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PathRestriction;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextContains;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextVisitor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

class IndexConfigGenerator {
    private QueryEngine queryEngine;
    private IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
    private final Set<String> propsWithFulltextConstraints = Sets.newHashSet();
    private Set<String> failedQueries = Sets.newHashSet();

    private NodeState INITIAL_CONTENT = createInitialContent();

    private NodeState createInitialContent() {
        NodeBuilder b = EMPTY_NODE.builder();
        new InitialContent().initialize(b);
        return squeeze(b.getNodeState());
    }

    public IndexConfigGenerator() {
        final Root root = new ImmutableRoot(INITIAL_CONTENT);
//		NodeState, Root, QueryEngineSettings, LuceneIndex, someting
        queryEngine = new QueryEngineImpl() {
            @Override
            protected ExecutionContext getExecutionContext() {
                return new ExecutionContext(INITIAL_CONTENT, root, new QueryEngineSettings(),
                        new LuceneIndexGeneratingIndexProvider(), null, null) {
                    @Override
                    public NodeTypeInfoProvider getNodeTypeInfoProvider() {
                        return DummyNodeTypeInfoProvider.INSTANCE;
                    }
                };
            }
        };
    }

    public void process(String statement) throws ParseException {
        String lang = statement.startsWith("/") ? "xpath" : "JCR-SQL2";
        process(statement, lang);
    }

    public void process(String statement, String language) throws ParseException {
        try {
            queryEngine.executeQuery(statement, "xpath", null, null);
        } catch (Exception e) {
            try {
                queryEngine.executeQuery(statement, "JCR-SQL2", null, null);
            } catch (Exception e1) {
                try {
                    queryEngine.executeQuery(statement, "sql", null, null);
                } catch (Exception e2) {
                    System.out.println(statement);
                    failedQueries.add(statement);
                } catch (AssertionError ae) {
                    failedQueries.add(statement);
                }
            } catch (AssertionError ae) {
                failedQueries.add(statement);
            }
        }
    }

    public NodeState getIndexConfig() {
        return builder.build();
    }

    private void processFilter(Filter filter, List<OrderEntry> sortOrder) {
        processPathRestriction(filter);
        IndexRule rule = processNodeTypeConstraint(filter);
        processFulltextConstraints(filter, rule);
        processPropertyRestrictions(filter, rule);
        processSortConditions(sortOrder, rule);
        processPureNodeTypeConstraints(filter, rule);

    }

    private void processPureNodeTypeConstraints(Filter filter, IndexRule rule) {
        if (filter.getFullTextConstraint() == null && filter.getPropertyRestrictions().isEmpty()
                && !"nt:base".equals(filter.getNodeType())) {
            rule.property("jcr:primaryType").propertyIndex();
        }
    }

    private void processFulltextConstraints(Filter filter, final IndexRule rule) {
        FullTextExpression ft = filter.getFullTextConstraint();
        if (ft == null) {
            return;
        }

        ft.accept(new FullTextVisitor.FullTextVisitorBase() {
            @Override
            public boolean visit(FullTextContains contains) {
                visitTerm(contains.getPropertyName());
                return true;
            }

            @Override
            public boolean visit(FullTextTerm term) {
                visitTerm(term.getPropertyName());
                return false;
            }

            private void visitTerm(String propertyName) {
                String p = propertyName;
                String propertyPath = null;
                String nodePath = null;
                if (p == null) {
                    return;
                }
                String parent = getParentPath(p);
                if (isNodePath(p)) {
                    nodePath = parent;
                } else {
                    propertyPath = p;
                }

                if (nodePath != null) {
                    builder.aggregateRule(rule.getRuleName()).include(nodePath).relativeNode();
                } else if (propertyPath != null) {
                    rule.property(propertyPath).analyzed();
                    propsWithFulltextConstraints.add(propertyPath);
                }
            }
        });
    }

    /**
     * In a fulltext term for jcr:contains(foo, 'bar') 'foo' is the property name.
     * While in jcr:contains(foo/*, 'bar') 'foo' is node name
     *
     * @return true if the term is related to node
     */
    private static boolean isNodePath(String fulltextTermPath) {
        return fulltextTermPath.endsWith("/*");
    }

    private void processSortConditions(List<OrderEntry> sortOrder, IndexRule rule) {
        if (sortOrder == null) {
            return;
        }

        for (OrderEntry o : sortOrder) {
            if ("jcr:score".equals(o.getPropertyName())) {
                continue;
            }

            if (o.getPropertyType().isArray()) {
                continue;
            }

            PropertyRule propRule = rule.property(o.getPropertyName());

            if (o.getPropertyType() != Type.UNDEFINED) {
                propRule.ordered(PropertyType.nameFromValue(o.getPropertyType().tag()));
            } else {
                propRule.ordered();
            }
        }
    }

    private void processPropertyRestrictions(Filter filter, IndexRule rule) {
        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
            // Ignore special restrictions
            if (isSpecialRestriction(pr)) {
                continue;
            }

            // QueryEngine adds a synthetic constraint for those properties
            // which are used in fulltext constraint so as to ensure that given
            // property is present. They need not be backed by index
            if (propsWithFulltextConstraints.contains(pr.propertyName)) {
                continue;
            }

            PropertyRule propRule = rule.property(pr.propertyName);
            if (pr.isNullRestriction()) {
                propRule.nullCheckEnabled();
            } else if (pr.isNotNullRestriction()) {
                propRule.notNullCheckEnabled();
            }
            propRule.propertyIndex();
        }

        if (filter.getPropertyRestriction(QueryConstants.RESTRICTION_LOCAL_NAME) != null) {
            rule.indexNodeName();
        }
    }

    private boolean isSpecialRestriction(PropertyRestriction pr) {
        String name = pr.propertyName;
        if (name.startsWith(":")) {
            return true;
        }
        if (name.startsWith("native*")) {
            return true;
        }
        return false;
    }

    private void processPathRestriction(Filter filter) {
        if (filter.getPathRestriction() != PathRestriction.NO_RESTRICTION
                || (filter.getPathRestriction() == PathRestriction.ALL_CHILDREN
                        && !PathUtils.denotesRoot(filter.getPath()))) {
            builder.evaluatePathRestrictions();
        }
    }

    private IndexRule processNodeTypeConstraint(Filter filter) {
        return builder.indexRule(filter.getNodeType());
    }

    private class LuceneIndexGeneratingIndexProvider implements QueryIndexProvider {
        @Override
        public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
            return ImmutableList.of(new LuceneIndexGeneratingIndex());
        }
    }

    private class LuceneIndexGeneratingIndex implements QueryIndex.AdvancedQueryIndex, QueryIndex {
        @Override
        public double getMinimumCost() {
            return 1.0;
        }

        @Override
        public double getCost(Filter filter, NodeState nodeState) {
            return Double.MAX_VALUE;
        }

        @Override
        public Cursor query(Filter filter, NodeState nodeState) {
            return null;
        }

        @Override
        public String getPlan(Filter filter, NodeState nodeState) {
            return null;
        }

        @Override
        public String getIndexName() {
            return "LuceneIndexGenerator";
        }

        @Override
        public List<QueryIndex.IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState rootState) {
            processFilter(filter, sortOrder);
            return Collections.emptyList();
        }

        @Override
        public String getPlanDescription(QueryIndex.IndexPlan plan, NodeState root) {
            return null;
        }

        @Override
        public Cursor query(QueryIndex.IndexPlan plan, NodeState rootState) {
            return null;
        }
    }

    private enum DummyNodeTypeInfoProvider implements NodeTypeInfoProvider {
        INSTANCE;

        @Override
        public NodeTypeInfo getNodeTypeInfo(String nodeTypeName) {
            return new DummyNodeTypeInfo(nodeTypeName);
        }
    }

    private static class DummyNodeTypeInfo implements NodeTypeInfo {
        private final String nodeTypeName;

        private DummyNodeTypeInfo(String nodeTypeName) {
            this.nodeTypeName = nodeTypeName;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public String getNodeTypeName() {
            return nodeTypeName;
        }

        @Override
        public Set<String> getSuperTypes() {
            return Sets.newHashSet();
        }

        @Override
        public Set<String> getPrimarySubTypes() {
            return Sets.newHashSet();
        }

        @Override
        public Set<String> getMixinSubTypes() {
            return Sets.newHashSet();
        }

        @Override
        public boolean isMixin() {
            return false;
        }

        @Override
        public Iterable<String> getNamesSingleValuesProperties() {
            return Sets.newHashSet();
        }
    }
}