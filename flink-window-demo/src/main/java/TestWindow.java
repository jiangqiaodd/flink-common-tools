import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TestWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<Tuple2<String, String>> src = environment.addSource(new SourceFunction<Tuple2<String, String>>() {
            @Override
            public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
                while (true) {
                    sourceContext.collect(Tuple2.of("man", "xjq"));
                    Thread.sleep(1000 * 2);
                }
            }

            @Override
            public void cancel() {

            }
        });

        src.keyBy(s -> s.f0).timeWindow(Time.seconds(7)).process(
                new ProcessWindowFunction<Tuple2<String, String>, Object, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, String>> iterable, Collector<Object> collector) throws Exception {
                        System.out.println(s);

                        System.out.println(context.window().getStart());
                    }


                });

        environment.execute("jobName");
    }
}
