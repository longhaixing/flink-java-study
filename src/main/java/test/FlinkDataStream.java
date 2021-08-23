package test;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.*;


public class FlinkDataStream {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stringDataStream = KafkaProp(env);
//        stringDataStream.print();

        DataStream<Object> output = stringDataStream.flatMap(new FlatMapFunction<String, Object>() {
            @Override
            public void flatMap(String s, Collector<Object> collector) throws Exception {
                collector.collect(s.split(","));
            }
        }).uid("split").filter(new FilterFunction<Object>() {
            @Override
            public boolean filter(Object o) throws Exception {
                for (String word : (String[]) o){
                    if (word.equalsIgnoreCase("a")) return true;
                    }
                return false;
                }
            }
        );


        output.print();

        env.execute("Flink-Kafka Demo");
    }

    private static DataStream<String> KafkaProp(StreamExecutionEnvironment env){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.51.113:9092");
        properties.setProperty("group.id", "ggg");
//        properties.setProperty("auto.offset.reset","earliest");

        return env.addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties));
    }

    private TableEnvironment TableEnvCreate(String TableName){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);
        StringBuilder CreateTable = new StringBuilder();
        CreateTable.append("CREATE TABLE ").append(TableName).append(" ").append("(");
        streamTableEnvironment.executeSql(String.valueOf(CreateTable));
        return streamTableEnvironment;
    }

}
