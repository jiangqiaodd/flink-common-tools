import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class HelloFlinkDataSet {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        String[] arr = new String[]{"one", "two", "three", "four"};
        List<String> list = Arrays.asList(arr);
        DataStreamSource<String> set = environment.fromCollection(list);
        set.print();

        environment.execute("hello, flink");

    }
}
