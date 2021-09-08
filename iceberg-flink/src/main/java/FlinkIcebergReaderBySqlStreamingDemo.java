import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalog;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

public class FlinkIcebergReaderBySqlStreamingDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment);

        String defaultSql = "select count(*),data from hadoop_cata.warehouse.table_a group by data";
        String defaultWarehouse = "file:///Users/game-netease/IdeaProjects/flink-common-tools/iceberg-flink/";
        String defaultDatabase = "warehouse";
        String defaultTableName = "table_a";


        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String sql = parameterTool.get("sql", defaultSql);
        String warehouse = parameterTool.get("warehouse", defaultWarehouse);
        String database = parameterTool.get("database", defaultDatabase);
        String tableName = parameterTool.get("tableName", defaultTableName);


        String localPath = warehouse + "/" + database + "/" + tableName;
        TableLoader loader = TableLoader.fromHadoopTable(localPath);


        // 1. DataStream reader -> table  -> table sql
        DataStream<RowData> rowDataDataStream = FlinkSource.forRowData()
                .tableLoader(loader)
                .env(environment)
                .streaming(false)
                .build();
        rowDataDataStream.print();
        tableEnv.createTemporaryView("table_a", rowDataDataStream);

        /* executeSql 默认转换成了 AppendStream
         error : org.apache.flink.table.api.TableException:
         toAppendStream doesn't support consuming update changes which is produced
         解决方案：解决方案：处理成 retreatStream
         */
        /*
        tableEnv.executeSql("SELECT count(*) from table_a").print();
        */

        Table table = tableEnv.sqlQuery("SELECT count(*) from table_a");
        tableEnv.toRetractStream(table, Row.class).print();


        // 2. catalog.database.table    sql
        Map<String, String> properties = new HashMap<>();
        properties.put("warehouse", warehouse);
        CatalogLoader catalogLoader = CatalogLoader.hadoop("hadoop_catalog", new Configuration(), properties);
        String[] namespaces = new String[]{};
        FlinkCatalog flinkCatalog = new FlinkCatalog("hadoop_catalog", database,
                namespaces, catalogLoader, true);

        System.out.println(String.valueOf(flinkCatalog.listDatabases()));
        System.out.println(String.valueOf(flinkCatalog.listTables(database)));

        tableEnv.registerCatalog("hadoop_cata", flinkCatalog);
        Table resultTable = tableEnv.sqlQuery(sql);
        tableEnv.toRetractStream(resultTable, Row.class).print();


        // retractStream 中， insert 是一条数据， 而 update 是两条数据
        // 流式读取，会

        environment.execute();
    }
}
