package org.janusgraph.datacreate;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.hadoop.MapReduceIndexManagement;

import java.util.Random;
import java.util.concurrent.ExecutionException;

public class GraphIndexUtils {

    private JanusGraph graph = GraphSingle.getGraphSingleInstance().getGraph();
    public static void main(String[] args) {

        GraphIndexUtils graphIndexUtils = new GraphIndexUtils();
        //new GraphIndexUtils().init();
        //new GraphIndexUtils().deleteCompositeIndex();

        //new GraphIndexUtils().insert();

        //graphIndexUtils.queryVertex();

        //重建索引数据
        graphIndexUtils.reindex();

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

        VertexLabel person = mgmt.makeVertexLabel("person").make();
        EdgeLabel father = mgmt.makeEdgeLabel("fiend").make();
        //PropertyKey rowKey = mgmt.makePropertyKey("rowKey").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey age = mgmt.makePropertyKey("age").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
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
     * 首先只针对单个属性建立索引
     */
    public void reindex() {
        graph.tx().rollback();
        JanusGraphManagement mgmt = graph.openManagement();
        //PropertyKey rowKey = graph.getPropertyKey("rowKey");
        PropertyKey rowKey = mgmt.getPropertyKey("name");
        mgmt.buildIndex("iname", Vertex.class).addKey(rowKey).buildMixedIndex("search");
        mgmt.commit();
        graph.tx().commit();

        graph.tx().rollback();
        // Block until the SchemaStatus transitions from INSTALLED to REGISTERED
        try {
            ManagementSystem.awaitGraphIndexStatus(graph, "iname").call();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Block until the SchemaStatus transitions from INSTALLED to REGISTERED");

        mgmt = graph.openManagement();
        MapReduceIndexManagement mr = new MapReduceIndexManagement(graph);
        try {
            mr.updateIndex(mgmt.getGraphIndex("iname"), SchemaAction.REINDEX).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (BackendException e) {
            e.printStackTrace();
        }
        mgmt.commit();
        System.out.println("execute reindex with mapreduce");
        //Enable the index
        mgmt = graph.openManagement();
        try {
            mgmt.updateIndex(mgmt.getGraphIndex("iname"), SchemaAction.ENABLE_INDEX).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        mgmt.commit();

        // Block until the SchemaStatus is ENABLED
        mgmt = graph.openManagement();
        try {
            ManagementSystem.awaitGraphIndexStatus(graph, "iname").status(SchemaStatus.ENABLED).call();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("index update to enable finish");
        mgmt.commit();
        graph.close();

    }

    /**
     * insert data
     */
    public void insert() {

        for (int i = 0; i < 100; i++){
            //JanusGraphManagement janusGraphManagement = graph.openManagement();
            Transaction tx = graph.tx();
            for(int j = 0; j < 100; j++){
                JanusGraphVertex p1 = graph.addVertex("person");
                p1.property("name", "lily " + System.currentTimeMillis());
                p1.property("age", new Random().nextInt(20) + 20);

                JanusGraphVertex p2 = graph.addVertex("person");
                p2.property("name", "smith " + System.currentTimeMillis());
                p2.property("age", new Random().nextInt(20) + 20);

                p1.addEdge("friend", p2);

            }
            tx.commit();
        }

        graph.close();
    }

    /**
     * QUERY
     * 如果不建立索引将会报如下异常
     * Query requires iterating over all vertices [(age = 25)]. For better performance, use indexes
     */
    public void queryVertex(){

        GraphTraversalSource g = graph.traversal();
        GraphTraversal<Vertex, Vertex> age = g.V().has("age", 25);
        while (age.hasNext()){
            System.out.println(age.next());
        }

        graph.close();
    }
}
