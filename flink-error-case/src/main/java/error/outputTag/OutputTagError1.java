package error.outputTag;

import org.apache.flink.shaded.curator4.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class OutputTagError1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        String[] arr = new String[]{"one", "two", "three", "four"};
        List<String> list = Arrays.asList(arr);
        DataStreamSource<String> set = environment.fromCollection(list);

        SingleOutputStreamOperator processStream = set.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, Context context, Collector<String> collector) throws Exception {
                Map demo1 = Maps.newHashMap();
                demo1.put("name", "xjq");
                demo1.put("date", LocalDate.now());

                // error case: 这里不实用匿名内部类，会报错，需要根据内部类里面绑定的范型生成相应的 TypeInformation
                context.output(new OutputTag<Map<String, Object>>("side-output"), demo1);

                // right case;
                /*
                context.output(new OutputTag<Map<String, Object>>("side-output") {}, demo1);
                */

                collector.collect(s + " by collect");
            }
        });

        processStream.print();

        DataStream<Map<String, Object>> sideStream =
                processStream.getSideOutput(new OutputTag<Map<String, Object>>("side-output") {
                });

        sideStream.print();

        environment.execute("hello, flink");

    }
}
