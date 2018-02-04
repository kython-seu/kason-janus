import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{ColumnRangeFilter, FilterList}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.SequenceFileInputFilter.Filter
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.janusgraph.core.{JanusGraph, JanusGraphTransaction}
import org.janusgraph.datacreate.GraphSingle
import org.janusgraph.diskstorage.{EntryList, ReadBuffer}
import org.janusgraph.diskstorage.hbase.HBaseKeyColumnValueStore
import org.janusgraph.diskstorage.util.{ReadArrayBuffer, StaticArrayBuffer, StaticArrayEntry, StaticArrayEntryList}
import org.janusgraph.graphdb.database.EdgeSerializer
import org.janusgraph.graphdb.database.idhandling.IDHandler
import org.janusgraph.graphdb.database.serialize.attribute.{IntegerSerializer, StringSerializer}
import org.janusgraph.graphdb.idmanagement.IDManager
import org.janusgraph.graphdb.relations.RelationCache
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx
import org.janusgraph.graphdb.types.SchemaSource.Entry
import org.janusgraph.model.GraphConfig

import scala.collection.JavaConverters._

/**
  * Created by zhangkai12 on 2018/2/3.
  */
object SparkGraphAnalysis {

    private val byte64: Array[Byte] = Array[Byte](64)
    private val byte96: Array[Byte] = Array[Byte](96)
    private val byte128: Array[Byte] = Array[Byte](-128)
    private val byte36: Array[Byte] = Array[Byte](36)
    private val byte37: Array[Byte] = Array[Byte](37)

    var idManager: IDManager = null
    var tx: StandardJanusGraphTx = null
    var gragh: JanusGraph = null
    val entryGetter = new HBaseKeyColumnValueStore.HBaseGetter(StaticArrayEntry.EMPTY_SCHEMA)
    var edgeSerializer: EdgeSerializer = null
    val stringSerializer: StringSerializer = new StringSerializer()
    val integerSerializer: IntegerSerializer = new IntegerSerializer()
    def main(args: Array[String]): Unit = {
        //val spark: SparkSession = SparkSession.builder().master("local[2]").appName("SparkJanusGraph").getOrCreate()
        init()
        val sconf = new SparkConf().setAppName("test").setMaster("local[4]")
        val sc = new SparkContext(sconf)

        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", GraphConfig.Config.HBASE_ADDR.getData)

        val scan: Scan = new Scan()
        scan.addFamily(Bytes.toBytes("e"))
        val myFilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE)
        myFilterList.addFilter(new ColumnRangeFilter(byte96, true, byte128, false)) //用户的边
        myFilterList.addFilter(new ColumnRangeFilter(byte64, true, byte96, false)) //用户的Property
        scan.setFilter(myFilterList)
        scan.setCaching(10000)
        val proto = ProtobufUtil.toScan(scan)
        conf.set(TableInputFormat.SCAN, Base64.encodeBytes(proto.toByteArray))
        conf.set(TableInputFormat.INPUT_TABLE, GraphConfig.Config.TABLE_NAME.getData)

        val resultRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
            classOf[ImmutableBytesWritable],
            classOf[Result])

        val rdd = resultRDD.map( res => {
            val rowKeyBytes = res._2.getRow
            val vertexId: Long = idManager.getKeyID(StaticArrayBuffer.of(rowKeyBytes)) //得到VertexId的值
            //import scala.collection.JavaConverters._
            val cfMap = res._2.getMap
            val colMap = cfMap.get(Bytes.toBytes("e")) //获取e列族的键值对值, key是列限定符(也是typeId), value是具体值
            val entrySet = colMap.entrySet()
            val iter = entrySet.iterator()
            while (iter.hasNext){
                val entry = iter.next()

                val key: Array[Byte] = entry.getKey

                //需要先查出类型
                val value = entry.getValue.lastEntry.getValue()
                val in: ReadBuffer = new ReadArrayBuffer(value)
                try {
                    println("***** " + entry.getValue.lastEntry.getKey + "========" + java.util.Arrays.toString(key) + "____value: " + stringSerializer.read(in))
                }catch {
                    case e: Exception => {


                        try{
                            println("error key is " + java.util.Arrays.toString(key) + "value " + integerSerializer.read(in))
                        }catch {
                            case e: Exception => {
                                println("error key is " + java.util.Arrays.toString(key))
                            }
                        }
                    }
                }
            }

            val entryList: EntryList = StaticArrayEntryList.ofBytes(colMap.entrySet, entryGetter)


        })
        println(rdd.count())
        rdd.persist()




        rdd.unpersist()
    }

    def init(): Unit ={
        gragh = GraphSingle.getGraphSingleInstance.getGraph
        tx = gragh.newTransaction().asInstanceOf[(StandardJanusGraphTx)]
        edgeSerializer = tx.getEdgeSerializer
        idManager = tx.getGraph.getIDManager
    }
}