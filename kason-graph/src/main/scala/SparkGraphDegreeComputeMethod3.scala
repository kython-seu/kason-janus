import java.io.File

import SparkGraphAnalysis.{edgeSerializer, gragh, idManager, tx}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{ColumnRangeFilter, FilterList}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.{GraphTraversal, GraphTraversalSource}
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.janusgraph.core.JanusGraph
import org.janusgraph.datacreate.GraphSingle
import org.janusgraph.diskstorage.hbase.HBaseKeyColumnValueStore
import org.janusgraph.diskstorage.util.{StaticArrayBuffer, StaticArrayEntry}
import org.janusgraph.graphdb.database.EdgeSerializer
import org.janusgraph.graphdb.database.idhandling.IDHandler.DirectionID
import org.janusgraph.graphdb.database.serialize.attribute.{IntegerSerializer, StringSerializer}
import org.janusgraph.graphdb.idmanagement.IDManager
import org.janusgraph.graphdb.idmanagement.IDManager.VertexIDType
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx
import org.janusgraph.model.GraphConfig

/**
  * 直接解析字節碼
  */

object SparkGraphDegreeComputeMethod3 {


    private val byte64: Array[Byte] = Array[Byte](64)
    private val byte96: Array[Byte] = Array[Byte](96)
    private val byte128: Array[Byte] = Array[Byte](-128)
    private val byte36: Array[Byte] = Array[Byte](36)
    private val byte37: Array[Byte] = Array[Byte](37)

    private val PREFIX_BIT_LEN: Int = 3

    var idManager: IDManager = null
    var tx: StandardJanusGraphTx = null
    var gragh: JanusGraph = null
    val entryGetter = new HBaseKeyColumnValueStore.HBaseGetter(StaticArrayEntry.EMPTY_SCHEMA)
    var edgeSerializer: EdgeSerializer = null
    val stringSerializer: StringSerializer = new StringSerializer()
    val integerSerializer: IntegerSerializer = new IntegerSerializer()
    def main(args: Array[String]): Unit = {

        init()
        val start: Long = System.currentTimeMillis()
        val sconf = new SparkConf().setAppName("test")
            .setMaster("local[4]")
            //.setMaster("spark://hdh123:7078")
            //.setJars(getSparkLibJars)
            //.setJars(getSparkLibJars)
            .set("spark.serializer",  "org.apache.spark.serializer.KryoSerializer")
            .set("spark.executor.memory", "10g")
            .set("spark.driver.memory", "2g")
            .set("spark.deploy.mode", "client")
            .set("spark.cores.max", "8")
            //.set("spark.cores.max", (StringUtils.defaultIfEmpty(sparkMaxCores, "2").toInt * SparkWorkerNodeUtils.getWorkerNodeNumber).toString)
            .set("spark.driver.maxResultSize", "2g")
            .set("spark.storage.memoryFraction",  "0.6")
            .set("spark.storage.safetyFraction",  "0.9")
            .set("spark.shuffle.memoryFraction", "0.4")
            .set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
            .set("spark.cleaner.referenceTracking.blocking.shuffle", "true")
            .set("spark.driver.allowMultipleContexts", "true")
        val sc = new SparkContext(sconf)

        val conf = HBaseConfiguration.create()
        val scan: Scan = new Scan()
        scan.addFamily(Bytes.toBytes("e"))
        val myFilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE)
        myFilterList.addFilter(new ColumnRangeFilter(byte96, true, byte128, false)) //用户的边
        myFilterList.addFilter(new ColumnRangeFilter(byte64, true, byte96, false)) //用户的Property
        scan.setFilter(myFilterList)
        scan.setCaching(10000)
        val proto = ProtobufUtil.toScan(scan)
        conf.set(TableInputFormat.SCAN, Base64.encodeBytes(proto.toByteArray))
        conf.set(TableInputFormat.INPUT_TABLE,GraphConfig.Config.TABLE_NAME.getData)
        val resultRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
            classOf[ImmutableBytesWritable],
            classOf[Result])

        val rdd: RDD[(Long, (Int, Int))] = resultRDD.map( res => {
            val rowKeyBytes = res._2.getRow
            //val userVertexId: Long = idManager.getKeyID(StaticArrayBuffer.of(rowKeyBytes)) //得到VertexId的值
            val userVertexId: Long = getUserVertexId(StaticArrayBuffer.of(rowKeyBytes))
            val cfMap = res._2.getMap
            val colMap = cfMap.get(Bytes.toBytes("e")) //获取e列族的键值对值, key是列限定符(也是typeId), value是具体值
            val iter = colMap.entrySet().iterator()
            var inDegreeNum: Int = 0
            var outDegreeNum: Int = 0
            while (iter.hasNext){
                val entry = iter.next()
                val key: Array[Byte] = entry.getKey  //是keyId, 就是那个属性顶点的vertexId, 也就是列限定符
                //这里只计算出入度, 属性信息全部不取
                val pre: Byte = key(0)

                val suffix: Byte = key(key.length-1)

                val flag: Int = ((pre >>> (8 - PREFIX_BIT_LEN)) & 1) << 1
                println("----suffix×××××：" + suffix)
                var dirId: Int = 0
                if(flag == 0){
                    dirId = 0
                }else{
                    dirId = flag + (suffix & 1)
                    println(" look 前三維 " + ((pre >>> (8 - PREFIX_BIT_LEN)) & 1) )
                }

                //val dirId: Int = (((pre >>> (8 - PREFIX_BIT_LEN)) & 1) << 1) + (suffix & 1)
                println("----dirId----" + dirId)
                //val keyBytes: ReadBuffer = StaticArrayBuffer.of(key).asReadBuffer()
                //val countPrefix: Array[Long] = VariableLong.readPositiveWithPrefix(keyBytes, PREFIX_BIT_LEN)
                //val dirId: Int = calInAndOutDirection(countPrefix)
                dirId match {
                    case 0 =>
                        DirectionID.PROPERTY_DIR
                        //println("DirectionID.PROPERTY_DIR")
                    case 2 =>
                        DirectionID.EDGE_OUT_DIR
                        //println("DirectionID.EDGE_OUT_DIR")
                        outDegreeNum = outDegreeNum + 1
                    case 3 =>
                        DirectionID.EDGE_IN_DIR
                        //println("DirectionID.EDGE_IN_DIR")
                        inDegreeNum = inDegreeNum + 1
                }
            }
            (userVertexId, (inDegreeNum, outDegreeNum))
        })


        println(rdd.count() )
        println("calculate cost : " + (System.currentTimeMillis() - start) )
        rdd.persist()
        rdd.foreach( res => {
            val vertexId: Long = res._1
            val degree: (Int, Int) = res._2
            /*val g: GraphTraversalSource = gragh.traversal()

            val t = g.V(vertexId).values("name")
            var name: String = ""
            if(t.hasNext){
                import scala.collection.Traversable._
                name = t.next()
            }
            t.close()
            tx.commit()*/
            println("顶点 " + vertexId  + " 入度 " + degree._1 +" 出度 " + degree._2)
        })

        //rdd.saveAsTextFile("hdfs://SERVICE-HADOOP-91d618f04a0d42c7a77aedd189c4f62c/test2")
        rdd.unpersist()

    }

    def calInAndOutDirection(countPrefix: Array[Long]) :Int={
        val relationType: Int = (countPrefix(1) & 1).toInt
        val direction: Int = (countPrefix(0) & 1).toInt

        val dirId: Int = (relationType << 1) + direction
        dirId
    }
    /**
      * 加入所有的tar包
      * @return 所有的tar包组成的集合
      */
    def getSparkLibJars: Array[String] = {
        val jarFilePath: String = "D:\\github_project\\grpah_20180207\\janusgraph-master\\SparkGraph\\graph-main\\target\\graph-main\\WEB-INF\\lib\\activation-1.1.jar"
        val jarFile: File = new File(jarFilePath)
        if (null == jarFile || !jarFile.isFile || !jarFile.getName.endsWith(".jar")) {
            //LOG.error("Spark jar文件路径：{}不正确", jarFile.getName)
            return null
        }
        val files: Array[File] = jarFile.getParentFile.listFiles
        println(files.length)
        if(files.isEmpty) {
            println("files empty")
            return null
        }
        val list: java.util.ArrayList[String] = new java.util.ArrayList[String]
        for (file <- files) {
            val filePath: String = file.getAbsolutePath
            System.out.println("JarName: " + filePath)
            if (filePath.toLowerCase.endsWith(".jar")) list.add(filePath)
        }
        list.toArray(new Array[String](list.size))
    }


    def getUserVertexId(staticArrayBuffer: StaticArrayBuffer): Long = {
        val partitionBits: Int = 5
        val partitionOffset: Long = java.lang.Long.SIZE - partitionBits

        val value: Long = staticArrayBuffer.getLong(0)
        if (VertexIDType.Schema.is(value)) {
            value
        } else {
            var theType: IDManager.VertexIDType = null
            import org.janusgraph.core.InvalidIDException
            import org.janusgraph.graphdb.idmanagement.IDManager.VertexIDType
            if (VertexIDType.NormalVertex.is(value))
                theType = VertexIDType.NormalVertex
            else if (VertexIDType.PartitionedVertex.is(value))
                theType = VertexIDType.PartitionedVertex
            else if (VertexIDType.UnmodifiableVertex.is(value))
                theType = VertexIDType.UnmodifiableVertex
            if (null == theType)
                throw new InvalidIDException("Vertex ID " + value + " has unrecognized type")
            var partition: Long = 0L
            if (partitionOffset < java.lang.Long.SIZE)
                partition = value >>> partitionOffset
            else
                partition = 0L

            val count: Long = (value >>> 3) & ((1L << (partitionOffset - 3)) - 1)
            var id: Long = (count << partitionBits) + partition
            if (theType != null) {
                id = theType.addPadding(id)
            }

            id
        }
    }

    def init(): Unit ={
        gragh = GraphSingle.getGraphSingleInstance.getGraph
        tx = gragh.newTransaction().asInstanceOf[(StandardJanusGraphTx)]
        edgeSerializer = tx.getEdgeSerializer
        idManager = tx.getGraph.getIDManager
    }
}
