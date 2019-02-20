package com.serli.oracle.of.bacon.repository;



import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.driver.v1.StatementResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Neo4JRepository {
    private final Driver driver;
    private final String KEVIN_BACON = "Bacon, Kevin (I)";

    public Neo4JRepository() {
        this.driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "linux"));
    }

    public List<GraphItem> getConnectionsToKevinBacon(String actorName) {
        Session session = driver.session();

        Transaction transaction = session.beginTransaction();
        StatementResult result = transaction.run("MATCH (source:Actors{name:'" + KEVIN_BACON + "'}), (destination:Actors{name:'" + actorName + "'}), "
						        				+ "path = shortestPath((source)-[:PLAYED_IN*]-(destination))"
                                                + "RETURN path");
        
        return result
        	.list()
        	.stream()
        	.flatMap(record -> record
        							.values()
        							.stream()
        							.map(Value::asPath)
        	)
        	.flatMap(path -> asGraphItems(path).stream())
        	.collect(Collectors.toList());
    }
    
    // Transform a path into a list of graph items. GraphNodes for nodes first, and then GraphEdge for relationships.
    private List<GraphItem> asGraphItems(Path path) {
        List<GraphItem> graphItems = asGraphItem(path.nodes(), this::asGraphItem);
        graphItems.addAll(asGraphItem(path.relationships(), this::asGraphItem));

        return graphItems;
    }

    private <T> List<GraphItem> asGraphItem(
            Iterable<T> iterable,
            Function<T, GraphItem> toGraphItem) {

        return StreamSupport
                .stream(iterable.spliterator(), false)
                .map(toGraphItem)
                .collect(Collectors.toList());
    }
    
    private GraphItem asGraphItem(Node n) {
        String type = n.labels().iterator().next();
        String property;
        if (type.equals("Actors")) {
        	property = "name";
        } else {
        	property = "title";
        }

        return new GraphNode(
        		n.id(), n.get(property).asString(), type
        );
    }

    private GraphItem asGraphItem(Relationship relationship) {
        return new GraphEdge(
               relationship.id(), relationship.startNodeId(), relationship.endNodeId(), relationship.type()
        );
    }

    public static abstract class GraphItem {
        public final long id;

        private GraphItem(long id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GraphItem graphItem = (GraphItem) o;

            return id == graphItem.id;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    private static class GraphNode extends GraphItem {
        public final String type;
        public final String value;

        public GraphNode(long id, String value, String type) {
            super(id);
            this.value = value;
            this.type = type;
        }
    }

    private static class GraphEdge extends GraphItem {
        public final long source;
        public final long target;
        public final String value;

        public GraphEdge(long id, long source, long target, String value) {
            super(id);
            this.source = source;
            this.target = target;
            this.value = value;
        }
    }
}
