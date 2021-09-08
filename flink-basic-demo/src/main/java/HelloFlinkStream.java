import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;

public class HelloFlinkStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> src = environment.addSource(new SourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                while (true) {
                    int rand = (int) (Math.random() * 5);
                    sourceContext.collect(rand);
                    Thread.sleep(1000 * 2);
                }
            }

            @Override
            public void cancel() {

            }
        });

        src.map(new MapFunction<Integer, Integer>() {
            Logger logger = LoggerFactory.getLogger("map");
            String[] arr = new String[] {"a", null, "c", null, "d"};
            @Override
            public Integer map(Integer value) throws Exception {
                try {
                    System.out.println(arr[value].toString());
                } catch (Exception e) {
                    StringWriter writer = new StringWriter();
                    PrintWriter printWriter = new PrintWriter(writer);
                    e.printStackTrace(printWriter);
                    logger.warn("exception: {}", writer.toString());
                }
                return value;
            }
        }).print();

        environment.execute("test-demo");
    }
}
