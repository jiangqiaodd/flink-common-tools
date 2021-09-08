import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import tableFunction.MyFlatMapFunction;
import tableFunction.MyMapFunction;

import java.time.ZoneOffset;

// 测试下常见 TABLE API 提供的操作
public class TableAPIDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(environment, settings);
        streamTableEnvironment.getConfig().setLocalTimeZone(ZoneOffset.ofHours(2));

        // 如果 Table API 想要使用 rowtime 那必须 assignTimestampsAndWatermarks
        DataStream dataStream = environment.fromElements(new Tuple2<>("key_A", 1627368287000L),
                new Tuple2<>("key_B", 1627368289000L),
                new Tuple2<>("key_B", 1627368289000L),
                new Tuple2<>("key_C", 1627368299000L),
                new Tuple2<>("key_C", 1627368299000L),
                new Tuple2<>("key_A", 1627368292000L),
                new Tuple2<>("key_C", 1627368379000L),
                new Tuple2<>("key_A", 1627368392000L),
                new Tuple2<>("key_B", 1627368439000L))
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2) {
                                return stringLongTuple2.f1;
                            }
                        }
                );

        Table tableB = streamTableEnvironment.fromDataStream(dataStream,
                Expressions.$("f0").as("field_A"),
//                Expressions.$("f1").cast(DataTypes.TIMESTAMP()).as("field_B"), 定义是不可以使用cast
                Expressions.$("f1").as("field_B"),
                Expressions.$("times").rowtime());
        tableB.printSchema();
        Table result = tableB.select(Expressions.$("*"));
//        streamTableEnvironment.toAppendStream(result, Row.class).print();
        /*
            key_A,1627368287000,2021-07-27T06:44:47
            key_B,1627368289000,2021-07-27T06:44:49
            key_C,1627368299000,2021-07-27T06:44:59
            key_A,1627368292000,2021-07-27T06:44:52
            key_C,1627368379000,2021-07-27T06:46:19
            key_A,1627368392000,2021-07-27T06:46:32
            key_B,1627368439000,2021-07-27T06:47:19
        */


        // 常规查询
        Table queryResult = tableB
                .filter(Expressions.or(Expressions.$("field_A").isEqual("key_A"),
                        Expressions.$("field_A").isEqual("key_B")))  // 过滤
                .select(Expressions.$("field_A").upperCase().as("KEY"),
                        Expressions.$("field_B").as("long_value"),
                        Expressions.$("field_B").toTimestamp().as("long2time"),
                        Expressions.$("times"))                           // 转换函数，相当于 map
                .window(                                                        // window + groupBY + select 缺一不可
                        Tumble.over(Expressions.lit(5).seconds())
                                .on(Expressions.$("times"))
                                .as("sec_window"))
                .groupBy(Expressions.$("KEY"), Expressions.$("sec_window"))
                .select(Expressions.$("KEY"),
                        Expressions.$("sec_window").start(),
                        Expressions.$("sec_window").end(),
                        Expressions.$("long_value").avg(),
                        Expressions.$("long_value").min(),
                        Expressions.$("long_value").count(),
                        Expressions.$("long_value").count().distinct());

//        streamTableEnvironment.toAppendStream(queryResult, Row.class).print();
        /*
        KEY_A,2021-07-27T06:44:45,2021-07-27T06:44:50,1627368287000,1627368287000,1,1
        KEY_B,2021-07-27T06:44:45,2021-07-27T06:44:50,1627368289000,1627368289000,2,1
        KEY_A,2021-07-27T06:44:50,2021-07-27T06:44:55,1627368292000,1627368292000,1,1
        KEY_A,2021-07-27T06:46:30,2021-07-27T06:46:35,1627368392000,1627368392000,1,1
        KEY_B,2021-07-27T06:47:15,2021-07-27T06:47:20,1627368439000,1627368439000,1,1
        */


        // row 自定义函数
        streamTableEnvironment.registerFunction("self_map", new MyMapFunction());

        Table res = tableB
                .map(Expressions.call("self_map", Expressions.$("field_A")))
//                .as("a", "b")     这里会根据 "self_map" 里面的 typeinformation 获取有哪些字段
                .select(Expressions.$("*"));
        streamTableEnvironment.registerTable("table_b", tableB);
        streamTableEnvironment.registerTable("res", res);
        System.out.printf("res schema : %s", res.getSchema().toString());
//        streamTableEnvironment.toAppendStream(res, Row.class).print();


        streamTableEnvironment.registerFunction("self_flatmap", new MyFlatMapFunction());
        res = tableB.flatMap(
                Expressions.call("self_flatmap", Expressions.$("field_A"), Expressions.$("field_B")))
                .select(Expressions.$("*"));
        streamTableEnvironment.toAppendStream(res, Row.class).print();
        /*
        +I[key_Aflatmap_one, 1627368287000]
        +I[key_Aflatmap_two, 1627368287000]
        +I[key_Bflatmap_one, 1627368289000]
        +I[key_Bflatmap_two, 1627368289000]
        *  */


        /*
         * 1. 使用自定义函数时，需要匹配上(例如 map(call(ScalarFunction)); flatMap(call(TableFunction)))，否则会出现计算结果不可预料的异常；
         * 2. 自定义函数的输出已经是整行了，这个和 sql 针对某一列产生某一列不同
         * 3. 如果想一行对多行，需要使用 flatMap
         * 4. 如果想要产生多列，需要自定函数的 输出数据类型为 Row, 输出时采用多个字段
         * */

        environment.execute("ss");

    }


}


