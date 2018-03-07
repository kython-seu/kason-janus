package graph.hbase;

import org.apache.commons.lang.StringUtils;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.hbase.HBaseStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.idassigner.placement.SimpleBulkPlacementStrategy;

public class HbaseSetup {
    public static ModifiableConfiguration getHBaseConfiguration(String tableName, String graphName) {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "hbase");
        if (!StringUtils.isEmpty(tableName)) config.set(HBaseStoreManager.HBASE_TABLE, tableName);
        if (!StringUtils.isEmpty(graphName)) config.set(GraphDatabaseConfiguration.GRAPH_NAME, graphName);
        config.set(GraphDatabaseConfiguration.TIMESTAMP_PROVIDER, HBaseStoreManager.PREFERRED_TIMESTAMPS);
        config.set(GraphDatabaseConfiguration.TIMESTAMP_PROVIDER, HBaseStoreManager.PREFERRED_TIMESTAMPS);
        config.set(SimpleBulkPlacementStrategy.CONCURRENT_PARTITIONS, 1);
        config.set(GraphDatabaseConfiguration.DROP_ON_CLEAR, false);

        return config;
    }
}
