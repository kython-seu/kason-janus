package org.janusgraph.datacreate;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.model.GraphConfig;


/**
 * Created by zhangkai12 on 2018/2/1.
 */
public class GraphSingle {

    private static JanusGraph graph;

    private GraphSingle() {
        graph = JanusGraphFactory
            .build()
            .set("storage.hbase.table", GraphConfig.Config.TABLE_NAME.getData())
            .set("storage.backend", "hbase")
            .set("storage.hostname", GraphConfig.Config.HBASE_ADDR.getData())
            .set("index.search.backend", "elasticsearch")
            .set("index.search.hostname", GraphConfig.Config.ES_ADDR.getData())
            .set("index.search.elasticsearch.create.ext.number_of_shards", "3")
            .set("index.search.elasticsearch.create.ext.number_of_replicas", "0")
            .open();
    }

    public JanusGraph getGraph() {
        return graph;
    }

    private static GraphSingle graphSingle;

    public static GraphSingle getGraphSingleInstance() {
        if (graphSingle == null) {
            synchronized (GraphSingle.class) {
                if (graphSingle == null) {
                    graphSingle = new GraphSingle();
                }
            }
        }

        return graphSingle;
    }

    public JanusGraphTransaction getTx() {
        return graph.newTransaction();
    }
}