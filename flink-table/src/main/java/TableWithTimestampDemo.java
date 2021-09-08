import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import pojo.Student;

import java.time.Duration;
import java.time.ZoneOffset;

public class TableWithTimestampDemo {
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
                                return stringLongTuple2.f1 + 1000;
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
        /*
            xjq,1627368287000,2021-07-27T06:44:48
            xxx,1627368289000,2021-07-27T06:44:50
        */


        // 使用已有的字段作为 rowtime, 会覆盖掉已有的字段, 该已有字段类型必须是 "Timestamp or Long"(并不会从该字段中提取 timestamp,
        // 还是源于TimestampsAndWatermarks)
        Table tableC = streamTableEnvironment.fromDataStream(dataStream, Expressions.$("f0"),
                Expressions.$("f1").rowtime().as("f1_rowtime"));
        tableC.printSchema();
        Table resultC = tableC.select(Expressions.$("*"));
        streamTableEnvironment.toAppendStream(resultC, Row.class).print();
        /*
            xjq,2021-07-27T06:44:48
            xxx,2021-07-27T06:44:50
        */


        environment.execute("ss");

    }


}
