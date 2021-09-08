import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SQLHelloWorld {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(environment, settings);

        Table result1 = streamTableEnvironment.sqlQuery("select 'hello world'");
        result1.execute().print();
        /*
         * hello world
         * */

        /*
        1. sqlQuery 只能有一条 select 语句，想要执行多条 sql 返回结果，可以用 executeSql
        Exception in thread "main" org.apache.flink.table.api.ValidationException: Unsupported SQL query!
        sqlQuery() only accepts a single SQL query of type SELECT, UNION, INTERSECT, EXCEPT, VALUES, and ORDER_BY.
         */

        TableResult result2 = streamTableEnvironment.executeSql("show functions");
        TableResult result3 = streamTableEnvironment.executeSql("select CURRENT_TIMESTAMP, 'add_column'");

        result2.print();
        /*
            +-----------------------+
            |         function name |
            +-----------------------+
            |                   mod |
            |                sha256 |
            |            dateFormat |
            |                   bin |
            |                 upper |
            |           currentDate |
            |            fromBase64 |
            |               degrees |
            ....
        *
        *  */
        result3.print();
        /*
            +-----------------------+--------------------------------+
            |     CURRENT_TIMESTAMP |                         EXPR$1 |
            +-----------------------+--------------------------------+
            | 2021-08-05T07:38:5... |                     add_column |
            +-----------------------+--------------------------------+
        *  */


    }


}
