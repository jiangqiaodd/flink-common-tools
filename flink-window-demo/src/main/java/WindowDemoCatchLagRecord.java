import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Calendar;

public class WindowDemoCatchLagRecord {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.getConfig().setAutoWatermarkInterval(10);
        System.out.println("auto watermark interval:" + environment.getConfig().getAutoWatermarkInterval());


        OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<Tuple2<String, Long>>("late record") {
        };

        DataStream<Tuple2<String, Long>> streamSource = environment.addSource(new SourceFunction<Tuple2<String, Long>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Long>> sourceContext) throws Exception {
                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950706000L));
                //Tue Jun 29 2021 15:11:46 GMT+0800 (中国标准时间)
                Thread.sleep(500);

                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950709000L));
                //Tue Jun 29 2021 15:11:49 GMT+0800 (中国标准时间)
                Thread.sleep(500);

                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950713000L));
                Thread.sleep(500);
                //Tue Jun 29 2021 15:11:53 GMT+0800 (中国标准时间)

                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950711000L));
                Thread.sleep(500);
                //Tue Jun 29 2021 15:11:51 GMT+0800 (中国标准时间)        虽然前面的水印高，但是还没有超过窗口最大值，因此该数据还能进入；

                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950753000L));
                //Tue Jun 29 2021 15:12:33 GMT+0800 (中国标准时间)
                Thread.sleep(500);

                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950706000L));
                //Tue Jun 29 2021 15:11:46 GMT+0800 (中国标准时间)        丢弃
                Thread.sleep(500);
                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950718000L));
                //Tue Jun 29 2021 15:11:58 GMT+0800 (中国标准时间)        丢弃
                Thread.sleep(500);
                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950719000L));
                //Tue Jun 29 2021 15:11:59 GMT+0800 (中国标准时间)        丢弃
                Thread.sleep(500);
                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950721000L));
                //Tue Jun 29 2021 15:12:01 GMT+0800 (中国标准时间)        丢弃
                Thread.sleep(500);
                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950717000L));
                //Tue Jun 29 2021 15:11:57 GMT+0800 (中国标准时间)        丢弃
                Thread.sleep(500);
                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950706000L));
                //Tue Jun 29 2021 15:11:46 GMT+0800 (中国标准时间)        丢弃

                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624961506000L));
                //Tue Jun 29 2021 18:11:46 GMT+0800 (中国标准时间)
            }

            @Override
            public void cancel() {

            }
        });

        DataStream<Tuple2<String, Long>> stream = streamSource.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> stringLongTuple2) {
                        return stringLongTuple2.f1;
                    }
                });

        Calendar calendar = Calendar.getInstance();

        SingleOutputStreamOperator dataStream = stream.keyBy(new KeySelector<Tuple2<String, Long>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
                return stringLongTuple2.f0;
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(outputTag)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> input, Tuple2<String, Long> output) throws Exception {
                        calendar.setTimeInMillis(output.f1);
                        Thread.sleep(1000);
                        System.out.println("param1:" + input);
                        System.out.println("param2:" + output + calendar.getTime());
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


        dataStream.print();

        dataStream.getSideOutput(outputTag).print();


        environment.setParallelism(1);
        environment.execute("demo");


        // 输出结果分析：
        /*
        now watermark:1624950712000
        7> ((xjq_xjq1624950709000,1624950706000),1624950709999,1624950705000,1624950710000)

        now watermark:1624950752000
        7> ((xjq_xjq1624950711000,1624950713000),1624950714999,1624950710000,1624950715000)

        now watermark:9223372036854775807
        7> ((xjq,1624950753000),1624950754999,1624950750000,1624950755000)

        now watermark:9223372036854775807
        7> ((xjq,1624961506000),1624961509999,1624961505000,1624961510000)

        正是丢弃的数据
        7> (xjq,1624950706000)
        7> (xjq,1624950718000)
        7> (xjq,1624950719000)
        7> (xjq,1624950721000)
        7> (xjq,1624950717000)
        7> (xjq,1624950706000)
        */

    }
}
