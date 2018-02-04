package org.janusgraph.base.operate;

import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.datacreate.GraphSingle;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.internal.JanusGraphSchemaCategory;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.util.stats.NumberUtil;

import java.util.Iterator;

public class BaseOperate {


    public static void main(String[] args) {
        //getVertexLabel("person");
        //baseKeyTest("person");
        getPartitionNum();

    }

    //根据点标签的名字获取点标签的点

    public static void getVertexLabel(String vertexLabelName){

        VertexLabel person = GraphSingle.getGraphSingleInstance().getTx().getVertexLabel("person");

        System.out.println(person.longId() + "---" + person.isPartitioned() + "-----" + person.name());

    }

    public static void baseKeyTest(String name){

        Iterable<JanusGraphVertex> person = GraphSingle.getGraphSingleInstance().getTx().query()
            .has(BaseKey.SchemaName.name(), Cmp.EQUAL, JanusGraphSchemaCategory.VERTEXLABEL.getSchemaName("person"))
            .vertices();

        Iterator<JanusGraphVertex> iterator = person.iterator();
        if(iterator.hasNext()){
            JanusGraphVertex next = iterator.next();

            System.out.println(next.longId());
        }

    }

    /**
     * 获取INdexSerializer
     */

    public IndexSerializer getIndexSerializer(){
        return ((StandardJanusGraph)GraphSingle.getGraphSingleInstance().getGraph()).getIndexSerializer();
    }

    public static int getPartitionNum(){
        int powerOf2 = NumberUtil.getPowerOf2(32);
        System.out.println(powerOf2);
        return powerOf2;
    }

}
