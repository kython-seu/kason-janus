/*
package org.janusgraph.es_operate;


import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.janusgraph.core.*;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;

import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.es.ElasticSearchClient;
import org.janusgraph.diskstorage.es.ElasticSearchIndex;
import org.janusgraph.diskstorage.es.ElasticSearchSetup;
import org.janusgraph.diskstorage.indexing.IndexInformation;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.GraphIndexStatusReport;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.model.GraphConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;



*/
/**
 * Created by zhangkai12 on 2018/1/31.
 *//*

public class GraphEs {
    private static final Logger logger = LoggerFactory.getLogger(GraphEs.class);
    private JanusGraph graph;
    private static final String indexName = "search";

    public GraphEs(){
        graph = JanusGraphFactory
            .build()
            .set("storage.hbase.table",GraphConfig.Config.TABLE_NAME.getData())
            .set("storage.backend", "hbase")
            .set("storage.hostname", GraphConfig.Config.HBASE_ADDR.getData())
            .set("index.search.backend","elasticsearch")
            .set("index.search.hostname",GraphConfig.Config.ES_ADDR.getData())
            .set("index.search.elasticsearch.create.ext.number_of_shards", "3")
            .set("index.search.elasticsearch.create.ext.number_of_replicas","1")
            .open();
    }

    public static void main(String[] args) {
        //new GraphEs().createSchema();
        //new GraphEs().deleteIndex();

        //new GraphEs().createHbaseIndex();
        */
/*try {
            new GraphEs().deleteHbaseIndex();
        } catch (Exception e) {
            e.printStackTrace();
        }*//*


        //new GraphEs().jianceHbaseIndex();

        */
/*try {
            new GraphEs().updateHbaseIndex();
        } catch (Exception e) {
            e.printStackTrace();
        }*//*


        GraphEs graphEs = new GraphEs();
        graphEs.createSchema();
        //IndexSerializer indexSerializer = graphEs.getIndexSerializer();
        //graphEs.testEsIndexSerializer(indexSerializer);
        //graphEs.deleteIndex();
        //graphEs.deleteEntity();
        //graphEs.addEntity();
        //graphEs.addLily();
        //graphEs.addNewProp();
        //graphEs.addIndexAfter();
    }

    */
/**
     * 当属性建立并插入数据之后,突然要插入索引
     *//*

    public void addIndexAfter(){
        //只能动态更新ES上的索引
        graph.tx().rollback();
        JanusGraphManagement mgmt = graph.openManagement();
//创建一个新的属性
        PropertyKey location = mgmt.getPropertyKey("locate");

        mgmt.buildIndex("locate", Vertex.class).addKey(location).buildMixedIndex("search");
        mgmt.commit();

        try {
            GraphIndexStatusReport locate = ManagementSystem.awaitGraphIndexStatus(graph, "locate").call();
            System.out.println("successed" + locate.getSucceeded());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        */
/*try {
            //Wait for the index to become available
            ManagementSystem.awaitGraphIndexStatus(graph, "locate").call();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*//*


        //Reindex the existing data
        mgmt = graph.openManagement();
        try {
            mgmt.updateIndex(mgmt.getGraphIndex("locate"),SchemaAction.REINDEX).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        mgmt.commit();

        graph.tx().commit();
        graph.close();
    }
    public void addLily(){
        JanusGraphTransaction tx = graph.newTransaction();
        JanusGraphVertex lily = tx.addVertex(LabelString.person.name());
        lily.property("rowKey","24");
        lily.property("name","lily smith");
        lily.property("age", 27);
        lily.property("locate", "hangzhou");
        tx.commit();
        graph.close();
    }
    public void addNewProp(){

        graph.tx().rollback();
        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey locateProp = mgmt.makePropertyKey("locate").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.commit();
        JanusGraphTransaction tx = graph.newTransaction();
        JanusGraphVertex lily = tx.addVertex(LabelString.person.name());
        lily.property("rowKey","24");
        lily.property("name","lily smith");
        lily.property("age", 27);
        lily.property("locate", "hangzhou");
        tx.commit();
        graph.close();
        //lily.addEdge(EdgeString.father.name(), father);

    }

    */
/**
     * 增加一个实体, 检查索引
     *//*

    public void addEntity(){

        JanusGraphTransaction tx = graph.newTransaction();
        JanusGraphVertex father = null;
        Iterator<JanusGraphVertex> nameFather = tx.query().has("name", "gelin smith").vertices().iterator();
        if(nameFather.hasNext()){
            father = nameFather.next();
        }
        JanusGraphVertex son = tx.addVertex(LabelString.person.name());
        son.property("rowKey","23");
        son.property("name","david smith");
        son.property("age", 27);
        son.addEdge(EdgeString.father.name(), father);
        tx.commit();
        graph.close();
    }
    */
/**
     * 删除rowKey 23的实体, 检查es属性索引是否删除数据, 测试发现删除的实体其索引上的数据会对应删除
     *//*

    public void deleteEntity(){
        Iterable<JanusGraphVertex> deletedEntity = graph.query().has("rowKey", "24").vertices();
        Iterator<JanusGraphVertex> del = deletedEntity.iterator();
        if(del.hasNext()){
            JanusGraphVertex next = del.next();
            next.remove();
        }
        graph.tx().commit();
        graph.close();
    }

    */
/*public void testEsIndexSerializer(IndexSerializer indexSerializer) {

        Map<String, ? extends IndexInformation> mixedIndexes = indexSerializer.mixedIndexes;

        for (Map.Entry<String, ? extends IndexInformation> entry : mixedIndexes.entrySet()){
            System.out.println("--------key " + entry.getKey() + " value " + entry.getValue());
        }
        boolean nameIndex = indexSerializer.containsIndex("name");
        System.out.println("---------------------------------" + nameIndex + "------------" + mixedIndexes.size());
        if(nameIndex){
            System.out.println("exist name index");
        }

    }
*//*


    public IndexSerializer getIndexSerializer(){
        IndexSerializer indexSerializer = ((StandardJanusGraph) graph).getIndexSerializer();
        return indexSerializer;
    }
    public void updateESIndex() throws Exception{
        //只能动态更新ES上的索引
        graph.tx().rollback();
        JanusGraphManagement mgmt = graph.openManagement();
//创建一个新的属性
        PropertyKey location = mgmt.makePropertyKey("location").dataType(Geoshape.class).make();
        JanusGraphIndex nameAndAge = mgmt.getGraphIndex("name");
//修改索引
        mgmt.addIndexKey(nameAndAge, location);
        mgmt.commit();
//Wait for the index to become available
        ManagementSystem.awaitGraphIndexStatus(graph, "name").call();
        // mgmt.
        //mgmt.awaitGraphIndexStatus(graph,"byNameUnique").call();
//Reindex the existing data
        mgmt = graph.openManagement();
        mgmt.updateIndex(mgmt.getGraphIndex("name"),SchemaAction.REINDEX).get();
        mgmt.commit();

        graph.tx().commit();
        graph.close();
    }
    public void jianceHbaseIndex() {
        graph.tx().rollback();
        JanusGraphManagement mgmt = graph.openManagement();
        JanusGraphIndex nameIndex = mgmt.getGraphIndex("byNameUnique");
        boolean byNameUnique = mgmt.containsGraphIndex("byNameUnique");
        if(byNameUnique){
            logger.info( " still have the hbase index " );
        }else {
            logger.info( " have already delete hbase index " );
        }
        mgmt.commit();
        graph.tx().commit();
        graph.close();
    }
    public void deleteHbaseIndex() throws Exception{
        graph.tx().rollback();
        JanusGraphManagement mgmt = graph.openManagement();
        JanusGraphIndex nameIndex = mgmt.getGraphIndex("byNameUnique");
        JanusGraphManagement.IndexJobFuture indexJobFuture = null;
        if(nameIndex.isCompositeIndex()){
            logger.info(" start to delete the hbase index ");
            mgmt.updateIndex(nameIndex, SchemaAction.DISABLE_INDEX).get();
            mgmt.commit();
            graph.tx().commit();
            // Block until the SchemaStatus transitions from INSTALLED to REGISTERED
            ManagementSystem.awaitGraphIndexStatus(graph, "byNameUnique").status(SchemaStatus.DISABLED).call();
            // Delete the index using MapReduceIndexJobs
            mgmt = graph.openManagement();
            nameIndex = mgmt.getGraphIndex("byNameUnique");
            indexJobFuture = mgmt.updateIndex(nameIndex, SchemaAction.REMOVE_INDEX);

            mgmt.commit();
            graph.tx().commit();
        }
        indexJobFuture.get();
        graph.close();
    }
    public void createHbaseIndex(){
        graph.tx().rollback();
        JanusGraphManagement mgmt = graph.openManagement();

        PropertyKey name = mgmt.getPropertyKey("name");
        JanusGraphIndex byNameUnique = mgmt.buildIndex("byNameUnique", Vertex.class).addKey(name).unique().buildCompositeIndex();
        if (byNameUnique.isCompositeIndex()){
            logger.info( " it is hbase index " );
        }
        mgmt.commit();
        graph.tx().commit();
        graph.close();
    }

    //delete index
    public void deleteIndex(){
        JanusGraphManagement mgmt = graph.openManagement();
        JanusGraphIndex nameIndex = mgmt.getGraphIndex("name");
        logger.info( " Index {}" , nameIndex);

        JanusGraphManagement.IndexJobFuture indexJobFuture = null;
        try {
            if (nameIndex.isMixedIndex()) {
                //System.out.println("it is es index");
                logger.info( " it is es index " );
                mgmt.updateIndex(nameIndex, SchemaAction.DISABLE_INDEX).get();
                mgmt.commit();
                graph.tx().commit();
                // Block until the SchemaStatus transitions from INSTALLED to REGISTERED
                ManagementSystem.awaitGraphIndexStatus(graph, "name").status(SchemaStatus.DISABLED).call();
                // Delete the index using MapReduceIndexJobs
                mgmt = graph.openManagement();
                nameIndex = mgmt.getGraphIndex("name");
                indexJobFuture = mgmt.updateIndex(nameIndex, SchemaAction.REMOVE_INDEX);
                indexJobFuture.get();
                mgmt.commit();
                graph.tx().commit();
            }else {
                //System.out.println("it is hbase second index");
                logger.info( " it is hbase second index " );
            }
        } catch (InterruptedException e) {
            logger.error( " DELETE ERROR ");
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {

            graph.close();
        }
       */
/* try {
            indexJobFuture.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }*//*


        mgmt = graph.openManagement();
        nameIndex = mgmt.getGraphIndex("name");
        if(nameIndex == null ){
            logger.info(" name es index has removed " );
        }
    }

    public void createSchema(){
        graph.tx().rollback();
        JanusGraphManagement mgmt = graph.openManagement();
        long start = System.currentTimeMillis();
        //Stopwatch stopwatch = Stopwatch.createStarted();

        logger.info(LOG_PREFIX + "CREATE Vertex Label" + LOG_SUFFIX);
        for (LabelString l : LabelString.values()){
            mgmt.makeVertexLabel(l.name()).make();
        }

        logger.info(LOG_PREFIX + " Create Edge Label" + LOG_SUFFIX);
        for (EdgeString e : EdgeString.values()){
            mgmt.makeEdgeLabel(e.name());
        }

        */
/**
         * mgmt.buildIndex("index1", Vertex.class).
         addKey(name, getStringMapping()).buildMixedIndex(INDEX);
         *//*


        List<JanusGraphIndex> list = new ArrayList<>();
        logger.info(LOG_PREFIX + "create Property and index" + LOG_SUFFIX);
        for(PropertyString p : PropertyString.values()){
            PropertyKey pk = mgmt.makePropertyKey(p.name()).dataType(p.getType()).cardinality(Cardinality.SINGLE).make();
            JanusGraphIndex janusGraphIndex = mgmt.buildIndex(p.name(), Vertex.class).addKey(pk).buildMixedIndex("search");
            list.add(janusGraphIndex);
        }
        //assertTrue(mgmt.containsGraphIndex("index1"));
        if(mgmt.containsGraphIndex("name")){
            logger.info(LOG_PREFIX + "contains name index" + LOG_SUFFIX);
        }
        JanusGraphIndex name = mgmt.getGraphIndex("name");
        if( name != null){
            logger.info(LOG_PREFIX +" INDEX {}" + LOG_SUFFIX, name.isMixedIndex());
        }else {
            logger.info(LOG_PREFIX +" NULL " + LOG_SUFFIX);
        }
        mgmt.commit();

        graph.tx().commit();

        JanusGraphTransaction tx = graph.newTransaction();

        for(int i = 0; i< 100000; i++) {

            JanusGraphTransaction tx2 = graph.newTransaction();
            JanusGraphVertex father = tx.addVertex(LabelString.person.name());
            father.property("rowKey", "1_" + System.currentTimeMillis());
            father.property("name", "gelin smith");
            father.property("age", 54);
            JanusGraphVertex children = tx.addVertex(LabelString.person.name());
            children.property("rowKey", "2_" + +System.currentTimeMillis());
            children.property("name", "lucy smith");
            children.property("age", 29);

            JanusGraphVertex son = tx.addVertex(LabelString.person.name());
            son.property("rowKey", "23_" + +System.currentTimeMillis());
            son.property("name", "david smith");
            son.property("age", 27);
            children.addEdge(EdgeString.father.name(), father);
            son.addEdge(EdgeString.father.name(), father);
            tx2.commit();

        }
        tx.commit();
        graph.close();

        logger.info(LOG_PREFIX + "time cost {}" + LOG_SUFFIX, (System.currentTimeMillis() - start));
    }


    public void test(){
        Settings settings = Settings.builder().put("cluster.name"
            ,"SERVICE-ELASTICSEARCH-edb1f61957e544e5b2da1ad6cef763ec").build();
        try {
            Client client = TransportClient.builder().settings(settings).build().addTransportAddress
                (new InetSocketTransportAddress(InetAddress.getByName("10.33.50.6"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.33.50.7"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.33.50.8"), 9300));
            IndicesExistsRequest inExistsRequest = new IndicesExistsRequest(indexName);

            IndicesExistsResponse inExistsResponse = client.admin().indices()
                .exists(inExistsRequest).actionGet();
            if(inExistsResponse.isExists()){
                System.out.println("index exists");
                DeleteIndexResponse deleteIndexResponse = client.admin().indices().prepareDelete(indexName) .execute().actionGet();

                if(deleteIndexResponse.isAcknowledged()){
                    System.out.println("delete successfully");
                }
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } finally {

        }
    }


}
*/
