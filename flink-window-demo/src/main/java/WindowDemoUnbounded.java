import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Properties;

public class WindowDemoUnbounded {
    public static void main(String[] args) throws Exception {

        /*
         * demo 1 中，可能由于 有界流数据，处理时间过短，没有达到生成水印的条件， 没有出现数据丢失的情形；这次直接消费 kafak
         *
         *  */
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        System.out.println("auto watermark interval:" + environment.getConfig().getAutoWatermarkInterval());


        String topic = "test_xjq";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "test01.brokers.canal.netease.com:9092");
        properties.setProperty("group.id", "test_consume_xjq");
        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty("flink.partition-discovery.interval-millis", "30000");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer(topic, new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();
        consumer.assignTimestampsAndWatermarks(
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)));


        DataStreamSource<String> streamSource = environment.addSource(consumer);

        DataStream<Tuple2<String, Long>> stream = streamSource.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                if (s == null || !s.contains(":")) {
                    return null;
                }
                String[] arr = s.split(":");
                long value = 0;
                try {
                    value = Long.parseLong(arr[1]);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return new Tuple2<String, Long>(arr[0], value);
            }
        });

        stream.print();

        DataStream out = stream.keyBy(new KeySelector<Tuple2<String, Long>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Long> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> input, Tuple2<String, Long> output) throws Exception {
                        System.out.println("param1:" + input);
                        return new Tuple2<>(input.f0 + "_" + output.f0 + output.f1, input.f1);
                    }
                }, new ProcessWindowFunction<Tuple2<String, Long>, Object, Object, TimeWindow>() {
                    @Override
                    public void process(Object o, Context context, Iterable<Tuple2<String, Long>> iterable, Collector<Object> collector) throws Exception {
                        System.out.println("now watermark:" + context.currentWatermark());
                        Tuple2<String, Long> value = iterable.iterator().next();
                        collector.collect(new Tuple4<>(value, context.window().maxTimestamp(), context.window().getStart(), context.window().getEnd()));
                    }
                });
        out.print();


        environment.setParallelism(1);
        environment.execute("demo");


    }
}
