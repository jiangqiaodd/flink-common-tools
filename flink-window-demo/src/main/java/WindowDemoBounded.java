import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Calendar;

public class WindowDemoBounded {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.getConfig().setAutoWatermarkInterval(10);
        System.out.println("auto watermark interval:" + environment.getConfig().getAutoWatermarkInterval());


        // 1. 该数据源头由于处理时间较短，因此产生的 watermark 无效, 不会造成 window 的清理，window 触发函数输出，通过一个 EventTimeTrigger 里面定时器 onEventTime 完成的；
        /*
        DataStreamSource<Tuple2<String, Long>> streamSource = environment.fromElements(
                new Tuple2<String, Long>("xjq", 1624950706000L),        //Tue Jun 29 2021 07:11:46 GMT+0000     1
                new Tuple2<String, Long>("xjq", 1624950709000L),        //Tue Jun 29 2021 07:11:49 GMT+0000     1
                new Tuple2<String, Long>("xjq", 1624950713000L),        //Tue Jun 29 2021 07:11:53 GMT+0000     2
                new Tuple2<String, Long>("xjq", 1624950753000L),        //Tue Jun 29 2021 07:12:33 GMT+0000     5
                new Tuple2<String, Long>("xjq", 1624950706000L),        //Tue Jun 29 2021 07:11:46 GMT+0000     1

                new Tuple2<String, Long>("xjq", 1624950718000L),        //Tue Jun 29 2021 07:11:58 GMT+0000     3
                new Tuple2<String, Long>("xjq", 1624950719000L),        //Tue Jun 29 2021 07:11:59 GMT+0000     3
                new Tuple2<String, Long>("xjq", 1624950721000L),        //Tue Jun 29 2021 07:12:01 GMT+0000     4
                new Tuple2<String, Long>("xjq", 1624950717000L),        //Tue Jun 29 2021 07:11:57 GMT+0000    3
                new Tuple2<String, Long>("xjq", 1624950706000L)         //Tue Jun 29 2021 07:11:46 GMT+0000       1
        );
        */
        /*
        输出结果：
        now watermark:9223372036854775807
        7> ((xjq_xjq1624950709000_xjq1624950706000_xjq1624950706000,1624950706000),1624950709999,1624950705000,1624950710000)
        now watermark:9223372036854775807
        7> ((xjq,1624950713000),1624950714999,1624950710000,1624950715000)
        now watermark:9223372036854775807
        7> ((xjq_xjq1624950719000_xjq1624950717000,1624950718000),1624950719999,1624950715000,1624950720000)
        now watermark:9223372036854775807
        7> ((xjq,1624950721000),1624950724999,1624950720000,1624950725000)
        now watermark:9223372036854775807
        7> ((xjq,1624950753000),1624950754999,1624950750000,1624950755000)
        */


        // 2. 修改数据源慢慢产生数据
        DataStream<Tuple2<String, Long>> streamSource = environment.addSource(new SourceFunction<Tuple2<String, Long>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Long>> sourceContext) throws Exception {
                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950706000L));
                Thread.sleep(500);
                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950709000L));
                Thread.sleep(500);
                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950713000L));
                Thread.sleep(500);
                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950753000L));
                Thread.sleep(500);
                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950706000L));

                Thread.sleep(500);
                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950718000L));
                Thread.sleep(500);
                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950719000L));
                Thread.sleep(500);
                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950721000L));
                Thread.sleep(500);
                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950717000L));
                Thread.sleep(500);
                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624950706000L));

                //多加一个元素 part 2.3
                sourceContext.collect(new Tuple2<String, Long>("xjq", 1624961506000L));  //Tue Jun 29 2021 18:11:46 GMT+0800 (中国标准时间)
            }

            @Override
            public void cancel() {

            }
        });

        // 2.1 不 sleep 可以看到，最后一个窗口没有更新 watermark
        /*
        now watermark:1624950752000
        7> ((xjq_xjq1624950709000_xjq1624950706000_xjq1624950706000,1624950706000),1624950709999,1624950705000,1624950710000)
        now watermark:1624950752000
        7> ((xjq,1624950713000),1624950714999,1624950710000,1624950715000)
        now watermark:1624950752000
        7> ((xjq_xjq1624950719000_xjq1624950717000,1624950718000),1624950719999,1624950715000,1624950720000)
        now watermark:1624950752000
        7> ((xjq,1624950721000),1624950724999,1624950720000,1624950725000)
        now watermark:9223372036854775807
        7> ((xjq,1624950753000),1624950754999,1624950750000,1624950755000)
        */

        // 2.2 sleep 50ms (等待 watermark 生成器 周期上报 watermark 完成)
        /*
            1. 第一第二条数据进入第一个时间窗口[1624950705000,1624950710000)：可见 第三条数据生成的 watermark 超过了 第一个时间区间最大值； 所以触发执行； 此时的 watermark 正是 1624950713000L(第三个元素) - 1000L
            2. 第三个元素进入第二个时间区间[1624950710000,1624950715000)； 第四个元素 1624950753000L - 1000L 进入后，watermark 更新水位，直接触发第二个区间进行计算
            3. 第四个元素进入后，此时水位已经超过到达 1624950752000， 在此之前的时间窗口，例如 [1624950715000,1624950720000), [1624950720000,1624950725000)
                还没有数据进入，但是 watermark 已经超过了 时间窗口的最大值，直接输出空值，后续的数据只能丢弃了;
            4. 第四个元素，进入的窗口为 [1624950750000,1624950755000), 由于没有更高的水位元素进入，会通过 EventTimeTrigger 定时触发的方式，一段时间后 触发窗口函数；
        */
        /*
        now watermark:1624950712000
        7> ((xjq_xjq1624950709000,1624950706000),1624950709999,1624950705000,1624950710000)

        now watermark:1624950752000
        7> ((xjq,1624950713000),1624950714999,1624950710000,1624950715000)

        now watermark:9223372036854775807
        7> ((xjq,1624950753000),1624950754999,1624950750000,1624950755000)
        */

        // 2.3 新增一条数据后最新数据后，1624961506000L
        /*
        now watermark:1624950712000
        7> ((xjq_xjq1624950709000,1624950706000),1624950709999,1624950705000,1624950710000)
        now watermark:1624950752000
        7> ((xjq,1624950713000),1624950714999,1624950710000,1624950715000)

        now watermark:1624961505000
        7> ((xjq,1624950753000),1624950754999,1624950750000,1624950755000)
        now watermark:9223372036854775807
        7> ((xjq,1624961506000),1624961509999,1624961505000,1624961510000)
        */

        DataStream<Tuple2<String, Long>> stream = streamSource.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> stringLongTuple2) {
                        return stringLongTuple2.f1;
                    }
                });

        Calendar calendar = Calendar.getInstance();

        stream.keyBy(new KeySelector<Tuple2<String, Long>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
                return stringLongTuple2.f0;
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
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
                })
                .print();


        environment.setParallelism(1);
        environment.execute("demo");
    }
}
