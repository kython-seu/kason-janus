package org.janusgraph.model;

public class GraphConfig {

    public static final String LOGGER_PREFIX = "------------------------------";

    public enum Config{


        HBASE_ADDR("192.168.1.105"),
        ES_ADDR("192.168.1.105:9200"),
        TABLE_NAME("graph");
        private String data;

        Config(String data){
            this.data = data;
        }

        public String getData(){
            return data;
        }
    }
}
