import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkKafka010Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(100 * 60);

        String topic = "test_xjq_office";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "xxx:9092");
        properties.setProperty("group.id", "test_consume");
        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty("flink.partition-discovery.interval-millis", "30000");
        String topicTwo = "test_xjq_three";
        /* *
        flink-kafka-connector010 不支持 kafka2.4.1 版本

        FlinkKafkaConsumer010 consumer = new FlinkKafkaConsumer010(topic, new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();
        environment.addSource(consumer).print();
        environment.execute("test flink kafka consumer010");
        // no result...
        */

        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer(topic, new SimpleStringSchema(), properties);
        consumer.setStartFromLatest();
        DataStream stream = environment.addSource(consumer).setParallelism(1).name("test_xjq-consume");
        stream.print().setParallelism(1);
        environment.setParallelism(1);
        environment.execute("flink consumer demo general");
    }
}
