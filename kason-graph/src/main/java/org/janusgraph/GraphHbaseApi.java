package org.janusgraph;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhangkai12 on 2018/1/6.
 */
public class GraphHbaseApi implements GraphInterface{

    private static Logger logger = LoggerFactory.getLogger(GraphHbaseApi.class);
    private static final String tableName = "graph";
    static JanusGraph graph;
    static {
        graph = JanusGraphFactory
            .build()
            .set("storage.hbase.table",tableName)
            .set("storage.backend", "hbase")
            .set("storage.hostname", "192.168.1.105")
            .set("index.search.backend","elasticsearch")
            .set("index.search.hostname", "192.168.1.105:9200")
            .open();
    }
    public static void main( String[] args )
    {

        System.out.println( "Hello World!" );

        /*JanusGraph graph = JanusGraphFactory
            .build()
            .set("storage.hbase.table",tableName)
            .set("storage.backend", "hbase")
            .set("storage.hostname", "10.33.50.6, 10.33.50.7, 10.33.50.8")
            .open();*/
        //GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true);

        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
        //tx.tr
        JanusGraphTransaction janusGraphTransaction = graph.newTransaction();
        //janusGraphTransaction

        GraphTraversalSource g = tx.traversal();

        GraphTraversal<Vertex, Vertex> hasSaturn = g.V().has("name", "saturn");
        Vertex saturmn = hasSaturn.next();
        GraphTraversal<Vertex, Object> values = g.V(saturmn).in("father").in("father").values("name");
        System.out.println(values.next());

        GraphTraversal<Edge, Edge> e = g.E();
        graph.close();
        //JanusGraph open = JanusGraphFactory.open("inmemory");

    }

    @Override
    public void initHbaseGraph() {
        /*JanusGraph graph = JanusGraphFactory
            .build()
            .set("storage.hbase.table",tableName)
            .set("storage.backend", "hbase")
            .set("storage.hostname", "10.33.50.6, 10.33.50.7, 10.33.50.8")
            .open();*/
        GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true);

    }
}
