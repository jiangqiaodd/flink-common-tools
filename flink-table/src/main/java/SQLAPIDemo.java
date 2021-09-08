import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.ZoneOffset;

public class SQLAPIDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(environment, settings);
        streamTableEnvironment.getConfig().setLocalTimeZone(ZoneOffset.ofHours(2));

        // 如果 Table API 想要使用 rowtime 那必须 assignTimestampsAndWatermarks
        DataStream dataStream = environment.fromElements(new Tuple2<>("xjq", 1627368287000L),
                new Tuple2<>("xxx", 1627368289000L))
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2) {
                                return stringLongTuple2.f1;
                            }
                        }
                );


        Table tableA = streamTableEnvironment.fromDataStream(dataStream);

        // 新字段 rowtime 使用 TimestampsAndWatermarks 中的 timestamp 和 watermark
        Table tableB = streamTableEnvironment.fromDataStream(dataStream,
                Expressions.$("f0").as("field_A"),
//                Expressions.$("f1").cast(DataTypes.TIMESTAMP()).as("field_B"), 定义是不可以使用cast
                Expressions.$("f1").as("field_B"),
                Expressions.$("times").rowtime());
        tableA.printSchema();
        tableB.printSchema();
        Table result = tableB.select(Expressions.$("*"));
        streamTableEnvironment.toAppendStream(result, Row.class).print();


        environment.execute("ss");

    }


}
