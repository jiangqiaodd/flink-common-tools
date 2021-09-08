import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OfficialDemoOne {
    public static final Logger LOG = LoggerFactory.getLogger(OfficialDemoOne.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        DataStream<Tuple2<Integer, String>> stream1 = env.fromElements(new Tuple2<>(1, "hello"));
        DataStream<Tuple2<Integer, String>> stream2 = env.fromElements(new Tuple2<>(1, "hello"));

// explain Table API
        Table table1 = tEnv.fromDataStream(stream1, Expressions.$("count"), Expressions.$("word"));
        Table table2 = tEnv.fromDataStream(stream2, Expressions.$("count"), Expressions.$("word"));
        Table table = table1
                .where(Expressions.$("word").like("F%"))
                .unionAll(table2);


        // 查看该 Table 的逻辑结构，和优化后的查询计划
        LOG.info("table query plan: {}", table.explain());


        // 查看 Table 的格式， 字段名由创建 Table 时指定， 字段类型由来源 DataStream 的类型自动生成
        LOG.info("table schema of table1: {}", table1.getSchema());
        LOG.info("table schema of table2: {}", table2.getSchema());
        LOG.info("table schema of table: {}", table.getSchema());

        // 查看 Table 上的操作
        LOG.info("table operator of table: {}", table.getQueryOperation());
        LOG.info("table operator of table1: {}", table1.getQueryOperation());

        /*  test-case:
            在 table/sql 程序中，通过 env.execute 时，也必须有一个 sink,
            这是和 纯 DataStream 代码中的区别，纯 DataStream 程序，可以有或者没有 sink
            当这种情况下，如果执行 env.execute() 会抛出异常：
                 No operators defined in streaming topology. Cannot execute.
        env.execute("dd");
         */

    }
}
