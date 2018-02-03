package org.janusgraph.graph.read;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.hbase.HBaseKeyColumnValueStore;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.janusgraph.graphdb.database.EdgeSerializer;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.relations.RelationCache;
import org.janusgraph.graphdb.relations.StandardEdge;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.vertices.StandardVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.janusgraph.model.GraphConfig.LOGGER_PREFIX;


/**
 * Created by zhangkai12 on 2018/2/1.
 */
public class HbaseAnalysis {
    private static byte[] byte64 = {64};
    private static byte[] byte96 = {96};
    private static byte[] byte128 = {-128};
    private static byte[] byte36 = {36};
    private static byte[] byte37 = {37};
    private static byte[] family = Bytes.toBytes("e");

    private final static Logger logger = LoggerFactory.getLogger(HbaseAnalysis.class);

    private HBaseKeyColumnValueStore.HBaseGetter entryGetter = new HBaseKeyColumnValueStore.HBaseGetter(StaticArrayEntry.EMPTY_SCHEMA);
    private Connection connection;
    private IDManager idManager;
    private String tableName;
    private EdgeSerializer edgeSerializer;
    private StandardJanusGraphTx tx;

    public HbaseAnalysis(StandardJanusGraphTx tx) throws Exception{
        if (!tx.configuration().getString("storage.backend").equals("hbase")) throw new Exception("storage.backend should be hbase");
        String hbaseZk = tx.configuration().getString("storage.hostname");
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", hbaseZk);
        this.connection = ConnectionFactory.createConnection(conf);
        this.idManager = tx.getGraph().getIDManager();
        this.tableName = tx.configuration().getString("storage.hbase.table");
        this.edgeSerializer = tx.getEdgeSerializer();
        this.tx = tx;
    }

    public void getRelations(Collection<? extends InternalVertex> vertices, int type) {

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Set<Long> getIds = new HashSet<>();
        boolean needEdge = (type&4)!=0;
        boolean needProperty = (type&2)!=0;
        boolean needLabel = (type&1)!=0;

        Map<Long, StandardVertex> vertexMap = new HashMap<>();
        for(InternalVertex v : vertices){
            long id = v.longId(); //得到vertexId
            getIds.add(id);
            StandardVertex vertex = new StandardVertex(tx, id, ElementLifeCycle.Loaded);
            if(!vertexMap.containsKey(id)){
                vertexMap.put(id, vertex);
            }
        }
        List<Get> gets = new ArrayList<>();
        FilterList filterList = new FilterList((FilterList.Operator.MUST_PASS_ONE));
        if (needEdge) filterList.addFilter(new ColumnRangeFilter(byte96, true, byte128, false));
        if (needProperty) filterList.addFilter(new ColumnRangeFilter(byte64, true, byte96, false));
        if (needLabel) filterList.addFilter(new ColumnRangeFilter(byte36, true, byte37, false));

        for (long id : getIds) {
            StaticBuffer rowKey = idManager.getKey(id);//根据id获取rowkey

            Get get = new Get(rowKey.as(StaticBuffer.ARRAY_FACTORY));

            get.addFamily(family).setFilter(filterList);

            gets.add(get);
        }

        if(gets.size() > 0){
            Table table = null;
            try {
                table = connection.getTable(TableName.valueOf(tableName));
                Result[] results = table.get(gets);
                for(Result result : results){
                    if(null != result){
                        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> hbasecf = result.getMap();
                        if(null != hbasecf){
                            long id = idManager.getKeyID(StaticArrayBuffer.of(result.getRow()));
                            NavigableMap<byte[], NavigableMap<Long, byte[]>> colMap = hbasecf.get(family);
                            EntryList entryList = StaticArrayEntryList.ofBytes(colMap.entrySet(), entryGetter);
                            Set<JanusGraphEdge> edges = new HashSet<>();
                            Set<JanusGraphVertexProperty> properties = new HashSet<>();
                            Set<JanusGraphVertex> vetexs = new HashSet<>();

                            for (Entry entry : entryList) {
                                RelationCache relation = edgeSerializer.parseRelation(entry, false, tx);
                                long typeId = relation.typeId;
                                //判断类型
                                if(IDManager.VertexIDType.UserEdgeLabel.is(typeId)) {
                                    EdgeLabel edgeLabel = (EdgeLabel) getRelationType(relation.typeId);
                                    System.out.println(edgeLabel.label());

                                    //StandardEdge standardEdge = new StandardEdge(relation.relationId, edgeLabel, vertexMap.get(id), relation.getValue(), ElementLifeCycle.Loaded);
                                    /*RelationCache userEdge = edgeSerializer.parseRelation(entry, true, tx);
                                    Object value = userEdge.getValue();
                                    logger.info(LOG_PREFIX + " user edge {} " + LOG_SUFFIX, value);*/
                                }
                                if (IDManager.VertexIDType.UserPropertyKey.is(typeId)) {
                                    PropertyKey propertyKey = (PropertyKey)getRelationType(relation.typeId);
                                    logger.info(LOGGER_PREFIX + "property value {}" + LOGGER_PREFIX, propertyKey.name() );
                                }
                                if (typeId==181) {
                                    VertexLabel vertexLabel = getExistVertexLabel(relation.getOtherVertexId());
                                    logger.info(LOGGER_PREFIX + "vertex label {}" + LOGGER_PREFIX , vertexLabel.name() );
                                }

                            }
                        }


                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if(null != table){
                    try {
                        table.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        stopWatch.stop();

        logger.info(LOGGER_PREFIX + " cost time {} " + LOGGER_PREFIX, stopWatch.getTime());
    }

    private RelationType getRelationType(long id){
        Vertex relationType = tx.getExistingRelationType(id);
        return (RelationType)relationType;
    }
    private VertexLabel getExistVertexLabel(long id) {
        VertexLabel vertexLabel = tx.getExistingVertexLabel(id);
        return vertexLabel;
    }
}