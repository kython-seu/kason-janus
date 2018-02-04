package org.janusgraph.datacreate;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.GraphHbaseApi;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.types.vertices.PropertyKeyVertex;
import org.janusgraph.graphdb.vertices.StandardVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphUtils {

    private static Logger logger = LoggerFactory.getLogger(GraphHbaseApi.class);
    private static final String tableName = "graph";
    private JanusGraph graph;
    
    public GraphUtils(){
        graph = JanusGraphFactory
            .build()
            .set("storage.hbase.table",tableName)
            .set("storage.backend", "hbase")
            .set("storage.hostname", "192.168.1.105")
            .set("index.search.backend","elasticsearch")
            .set("index.search.hostname", "192.168.1.105:9200")
            .set("index.search.elasticsearch.create.ext.number_of_shards", "3")
            .set("index.search.elasticsearch.create.ext.number_of_replicas","0")
            .open();
    }

    public static void main(String[] args) {
        new GraphUtils().addOne();
    }


    public void addOne(){
        JanusGraphTransaction tx = graph.newTransaction();
        JanusGraphVertex fv = tx.addVertex(T.label, "person", "name", "david even", "age", 48, "rowKey", "3");
        JanusGraphVertex dv = tx.addVertex(T.label, "person", "name", "lily even", "age", 24, "rowKey", "4");
        JanusGraphEdge father1 = dv.addEdge("father", fv);
        tx.commit();
        tx.close();
        graph.close();
    }
    
    public void init(){
        graph.tx().rollback();
        JanusGraphManagement mgmt = graph.openManagement();

        VertexLabel person = mgmt.makeVertexLabel("person").make();
        EdgeLabel father = mgmt.makeEdgeLabel("father").make();
        PropertyKey rowKey = mgmt.makePropertyKey("rowKey").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex("rowKey", Vertex.class).addKey(rowKey).buildMixedIndex("search");

        PropertyKey age = mgmt.makePropertyKey("age").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex("age", Vertex.class).addKey(age).buildMixedIndex("search");

        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex("name", Vertex.class).addKey(name).buildMixedIndex("search");

        mgmt.commit();

        //add data

        JanusGraphTransaction tx = graph.newTransaction();
        JanusGraphVertex fv = tx.addVertex(T.label, "person", "name", "frank smith", "age", 55, "rowKey", "1");
        JanusGraphVertex dv = tx.addVertex(T.label, "person", "name", "lucy smith", "age", 26, "rowKey", "2");
        JanusGraphEdge father1 = dv.addEdge("father", fv);
        tx.commit();
        tx.close();
        graph.close();

    }
    
}
