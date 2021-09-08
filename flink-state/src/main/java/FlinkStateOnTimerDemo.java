import function.MyProcessFunction;
import function.MyProcessWindowFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class FlinkStateOnTimerDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.enableCheckpointing(1000 * 10);

        DataStream<String> stream = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (true) {
                    sourceContext.collect("hello world");
                    Thread.sleep(2000);
                }
            }

            @Override
            public void cancel() {

            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(
                Time.seconds(1)) {
            @Override
            public long extractTimestamp(String s) {
                return System.currentTimeMillis() - 6 * 1000;
            }
        });

        // 不设置 env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)，
        // timestamp 和 watermark 不起作用的
        /*
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        stream.keyBy(String::toString)
                .process(new MyProcessFunction()).print();
        */


        // 使用 TumblingEventTimeWindows 也必须设置 时间属性 EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        stream.keyBy(String::toString)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new MyProcessWindowFunction()).disableChaining()
                .keyBy(String::toString)
                .process(new MyProcessFunction())
                .print()
        ;

        env.execute("test ");
    }
}
