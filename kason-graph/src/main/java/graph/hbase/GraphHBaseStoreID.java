package graph.hbase;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.IDAuthority;
import org.janusgraph.diskstorage.IDBlock;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.hbase.HBaseStoreManager;
import org.janusgraph.diskstorage.idmanagement.ConflictAvoidanceMode;
import org.janusgraph.diskstorage.idmanagement.ConsistentKeyIDAuthority;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.idassigner.IDBlockSizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID;
import static org.junit.Assert.*;

public class GraphHBaseStoreID {
    private static final Logger log = LoggerFactory.getLogger(GraphHBaseStoreID.class);
    private KeyColumnValueStoreManager manager;
    private IDAuthority idAuthority;

    private WriteConfiguration baseStoreConfiguration; //创建基本的WriteConfiguration


    private final int uidBitWidth;
    private final boolean hasFixedUid;
    private final boolean hasEmptyUid;
    private final long blockSize;
    private final long idUpperBoundBitWidth;
    private final long idUpperBound;
    private Configuration config;

    private static final Duration GET_ID_BLOCK_TIMEOUT = Duration.ofMillis(300000L);//获取ID的超时时间
    public GraphHBaseStoreID() {
        baseStoreConfiguration = new CommonsConfiguration();
        config = new ModifiableConfiguration(ROOT_NS,baseStoreConfiguration, BasicConfiguration.Restriction.NONE);
        blockSize = config.get(GraphDatabaseConfiguration.IDS_BLOCK_SIZE);
        idUpperBoundBitWidth = 30;
        idUpperBound = 1L <<idUpperBoundBitWidth;

        uidBitWidth = config.get(IDAUTHORITY_CAV_BITS);//这个就是uniqueIdBitWidth = 4  uniqueIDUpperBound 则是1 << uniqueIdBitWidth = 16
        //hasFixedUid = !config.get(IDAUTHORITY_RANDOMIZE_UNIQUEID);
        hasFixedUid = !ConflictAvoidanceMode.GLOBAL_AUTO.equals(config.get(IDAUTHORITY_CONFLICT_AVOIDANCE));
        hasEmptyUid = uidBitWidth==0;
    }

    public static void main(String[] args) {
       /* Configuration config = ((StandardJanusGraph) GraphSingle.getGraphSingleInstance().getGraph()).getConfiguration().getConfiguration();
        int uidBitWidth = config.get(IDAUTHORITY_CAV_BITS);
        System.out.println(uidBitWidth);
*/
        //HbaseStoreManagerUse();
        try {
            GraphHBaseStoreID hBaseStoreTest = new GraphHBaseStoreID();
            hBaseStoreTest.open("");
            hBaseStoreTest.idAcquire();
        } catch (BackendException e) {
            e.printStackTrace();
        }
    }

    public void open(String tableName) throws BackendException {

        ModifiableConfiguration sc = new ModifiableConfiguration(ROOT_NS,baseStoreConfiguration.copy(), BasicConfiguration.Restriction.NONE);
        if (!sc.has(UNIQUE_INSTANCE_ID)) {//JanusGraph instance 唯一标识符
            String uniqueGraphId = getOrGenerateUniqueInstanceId(sc);
            log.warn("Setting unique instance id: {}", uniqueGraphId);
            sc.set(UNIQUE_INSTANCE_ID, uniqueGraphId);
        }
        sc.set(GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS,4);//设置最大分区4位, 默认是32位置的也就是2的5次方 32, 此处只需要2的2次方为4 所以partitionBitWidth = 2,
        manager = openStorageManager(tableName);
        KeyColumnValueStore idStore = manager.openDatabase("ids");
        StoreFeatures storeFeatures = manager.getFeatures();
        if(storeFeatures.isKeyConsistent()){
            System.out.println(" yes we can open the id store");
            idAuthority = new ConsistentKeyIDAuthority(idStore, manager, sc);
        }else {
            throw new IllegalArgumentException("Cannot open id store");
        }
    }

    private void HbaseStoreManagerUse() {
        HBaseStoreManager hBaseStoreManager = (HBaseStoreManager)openStorageManager("hiki");
        try {
            boolean exists = hBaseStoreManager.exists();
            System.out.println("exists ? " + exists);
        } catch (BackendException e) {
            e.printStackTrace();
        }

        DistributedStoreManager.Deployment deployment = hBaseStoreManager.getDeployment();
        System.out.println("deploy name ----" + deployment.name());


        try {
            List<KeyRange> localKeyPartition = hBaseStoreManager.getLocalKeyPartition();
            System.out.println(localKeyPartition.size());

        } catch (BackendException e) {
            e.printStackTrace();
        }
    }


    public KeyColumnValueStoreManager openStorageManager(String tableName) {
        HBaseStoreManager hiki = null;
        try {
            hiki = new HBaseStoreManager(HbaseSetup.getHBaseConfiguration(tableName, null));
        } catch (BackendException e) {
            e.printStackTrace();
        }
        return hiki;
    }


    private class InnerIDBlockSizer implements IDBlockSizer {

        @Override
        public long getBlockSize(int idNamespace) {
            return blockSize;
        }

        @Override
        public long getIdUpperBound(int idNamespace) {
            return idUpperBound;
        }
    }

    public void idAcquire() throws BackendException{
        final IDBlockSizer blockSizer = new InnerIDBlockSizer();
        idAuthority.setIDBlockSizer(blockSizer);
        int numTrials = 100;// 测试生产num个id
        LongSet ids = new LongHashSet((int)blockSize*numTrials);
        long previous = 0;
        for(int i = 0; i< numTrials; i++){
            IDBlock block = idAuthority.getIDBlock(0, 0, GET_ID_BLOCK_TIMEOUT);
            checkBlock(block,ids);
            if (hasEmptyUid) {
                log.warn("now it has empty uid");
                if (previous!=0)
                    assertEquals(previous+1, block.getId(0));
                previous=block.getId(block.numIds()-1);
            }
        }
    }

    private void checkBlock(IDBlock block, LongSet ids) {
        assertEquals(blockSize,block.numIds());
        for (int i=0;i<blockSize;i++) {
            long id = block.getId(i);
            log.warn("get id {}", id );
            assertEquals(id,block.getId(i));
            assertFalse(ids.contains(id));
            assertTrue(id<idUpperBound);
            assertTrue(id>0);
            ids.add(id);
        }
        if (hasEmptyUid) {
            assertEquals(blockSize-1,block.getId(block.numIds()-1)-block.getId(0));
        }
        try {
            block.getId(blockSize);
            fail();
        } catch (ArrayIndexOutOfBoundsException ignored) {}
    }
}
