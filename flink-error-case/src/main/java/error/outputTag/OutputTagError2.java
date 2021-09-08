package error.outputTag;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.curator4.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class OutputTagError2 {
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
                context.output(new OutputTag<Map<String, Object>>("side-output") {
                }, demo1);
                collector.collect(s + " by collect");
            }
        });

        processStream.print();

        DataStream<Map<String, Object>> sideStream =
                processStream.getSideOutput(new OutputTag<Map<String, Object>>("side-output") {
                });
        sideStream.map(new MapFunction<Map<String, Object>, Object>() {
            @Override
            public Object map(Map<String, Object> stringObjectMap) throws Exception {
                System.out.println(stringObjectMap.get("date"));
                Object object = stringObjectMap.get("date");

                /*
                error case: 虽然是这里转型错误，但是错误信息会抛出在 context.output(OutputTag, record) 里面进行抛出, 不一定会指向这行代码；
                异常信息只会反映在 output(OutputTag, record) 中；

                Caused by: java.lang.ClassCastException: java.time.LocalDate cannot be cast to java.time.ZonedDateTime.
                Failed to push OutputTag with id 'side-output' to operator.
                This can occur when multiple OutputTags with different types but identical names are being used.
                    at org.apache.flink.streaming.runtime.tasks.OperatorChain$CopyingChainingOutput.pushToOperator(OperatorChain.java:722)
                    at org.apache.flink.streaming.runtime.tasks.OperatorChain$CopyingChainingOutput.collect(OperatorChain.java:703)
                    at org.apache.flink.streaming.runtime.tasks.OperatorChain$BroadcastingOutputCollector.collect(OperatorChain.java:794)
                    at org.apache.flink.streaming.api.operators.CountingOutput.collect(CountingOutput.java:58)
                    at org.apache.flink.streaming.api.operators.ProcessOperator$ContextImpl.output(ProcessOperator.java:102)
                    at error.outputTag.OutputTagError2$1.processElement(OutputTagError2.java:34)
                    at error.outputTag.OutputTagError2$1.processElement(OutputTagError2.java:28)
                */
                System.out.println(object);
                return "dddd";
            }
        }).print();

        environment.execute("hello, flink");

    }
}
