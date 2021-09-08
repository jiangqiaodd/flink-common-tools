import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OfficialDemoTwo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        DataStream<Tuple2<Integer, String>> stream1 = env.fromElements(new Tuple2<>(1, "hello"))
                .returns(new TypeHint<Tuple2<Integer, String>>() {
                });
        DataStream<Tuple2<Integer, String>> stream2 = env.fromElements(new Tuple2<>(2, "world"));

// explain Table API
        Table table1 = tEnv.fromDataStream(stream1, Expressions.$("count"), Expressions.$("word"));
        Table table2 = tEnv.fromDataStream(stream2, Expressions.$("count"), Expressions.$("word"));
        Table table = table1.unionAll(table2);

        DataStream stream = tEnv.toAppendStream(table, TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {
        }));
        stream.print();
        env.execute("dataStream -> Table -> dataStream");
    }
}
