package org.janusgraph.datacreate;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.hadoop.MapReduceIndexManagement;

import java.util.concurrent.ExecutionException;

public class GraphIndexUtils {

    private JanusGraph graph = GraphSingle.getGraphSingleInstance().getGraph();
    public static void main(String[] args) {

        new GraphIndexUtils().init();
        //new GraphIndexUtils().deleteCompositeIndex();


    }


    public void getId() {
        PropertyKey rowKey = graph.getPropertyKey("rowKey");
        System.out.println(rowKey.longId());

    }
    public void deleteCompositeIndex(){

        JanusGraphManagement m = graph.openManagement();
        JanusGraphIndex nameIndex = m.getGraphIndex("secrowKey");
        if(nameIndex.isCompositeIndex()){
            System.out.println(" it is composite index ");
        }
        try {
            m.updateIndex(nameIndex, SchemaAction.DISABLE_INDEX).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        m.commit();
        graph.tx().commit();

        try {
            ManagementSystem.awaitGraphIndexStatus(graph, "secrowKey").status(SchemaStatus.DISABLED).call();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("put index to disable");
        // Delete the index using JanusGraphManagement
        m = graph.openManagement();
        nameIndex = m.getGraphIndex("secrowKey");
        JanusGraphManagement.IndexJobFuture future = m.updateIndex(nameIndex, SchemaAction.REMOVE_INDEX);
        m.commit();
        graph.tx().commit();

        try {
            future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println("delete composite index finished");
        graph.close();
    }
    public void init(){
        graph.tx().rollback();
        JanusGraphManagement mgmt = graph.openManagement();

        //VertexLabel person = mgmt.makeVertexLabel("person").make();
        //EdgeLabel father = mgmt.makeEdgeLabel("fiend").make();
        PropertyKey rowKey = mgmt.makePropertyKey("rowKey").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        //mgmt.buildIndex("rowKey", Vertex.class).addKey(rowKey).buildMixedIndex("search");
        //mgmt.buildIndex("secrowKey", Vertex.class).addKey(rowKey).buildCompositeIndex();

        /*PropertyKey age = mgmt.makePropertyKey("age").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex("age", Vertex.class).addKey(age).buildMixedIndex("search");

        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex("name", Vertex.class).addKey(name).buildMixedIndex("search");*/

        mgmt.commit();

        //add data

        /*JanusGraphTransaction tx = graph.newTransaction();
        JanusGraphVertex fv = tx.addVertex(T.label, "person", "name", "frank smith", "age", 55, "rowKey", "1");
        JanusGraphVertex dv = tx.addVertex(T.label, "person", "name", "lucy smith", "age", 26, "rowKey", "2");
        JanusGraphEdge father1 = dv.addEdge("father", fv);
        tx.commit();
        tx.close();*/
        graph.tx().commit();
        graph.close();

    }

    public void addHBaseIndex(){

        graph.tx().rollback();
        JanusGraphManagement mgmt = graph.openManagement();
        //PropertyKey rowKey = graph.getPropertyKey("rowKey");
        PropertyKey rowKey = mgmt.makePropertyKey("rowKey").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex("secrowKey", Vertex.class).addKey(rowKey).buildCompositeIndex();
        mgmt.commit();
        graph.tx().commit();
        graph.close();
    }

    /**
     * reindex
     */
    public void reindex() {

    }
}
