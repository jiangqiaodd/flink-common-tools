package error.pressure;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 模拟背压: 界面观测到的背压算子，一般是其下游处理慢了。
 * 定位时，先找到界面中背压红色的算子，然后一个一个向下找，当下游显示没有背压时，表示此时查看的节点正是处理慢的节点。
 */
public class FlinkPressureDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataSource = environment.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (true) {
                    Thread.sleep(10);
                    String value = "start...";
                    for (int index = 0; index < 1000; index++) {
                        value += "hello world";
                    }
                    sourceContext.collect(value + System.currentTimeMillis());
                }
            }

            @Override
            public void cancel() {
            }
        });

        dataSource.print().setParallelism(1).disableChaining();

        dataSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String o) throws Exception {
                Thread.sleep(1000 * 30);
                return o.substring(10, 20) + " lag 10s";
            }
        }).setParallelism(8)
                .print().setParallelism(1).disableChaining();

        environment.execute("test pressure");
    }
}
