import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.Actions;

public class JobEntry {
    public static final String HDFS_WAREHOUSE_KEY = "catalog.hdfs.warehouse";

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String warehouse = parameterTool.getRequired(HDFS_WAREHOUSE_KEY);
        System.out.println(warehouse);

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        TableLoader tableLoader = TableLoader.fromHadoopTable(warehouse);
        tableLoader.open();
        Table table = tableLoader.loadTable();
        System.out.println(table.schema());

        /*
         * * 压缩数据文件 /data/下的文件
         *
         * 将 parquet 文件合并生成新的文件；旧文件会删除么？
         */
        RewriteDataFilesActionResult result = Actions.forTable(environment, table)
                .rewriteDataFiles()
                .maxParallelism(1)
                .targetSizeInBytes(128 * 1024 * 1024)
                .execute();
        System.out.println(result.deletedDataFiles());
        System.out.println(result.addedDataFiles());
        /*
            * 清理 metadata 文件，主要是通过 table 自己的 write 参数控制
            * Remove old metadata files
            *
            *
            write.metadata.delete-after-commit.enabled	Whether to delete old metadata files after each table commit
            write.metadata.previous-versions-max	The number of old metadata files to keep
        */
//        System.out.println(table.properties());
//        table.updateProperties().set("write.metadata.delete-after-commit.enabled", "true")
//                .set("write.metadata.previous-versions-max", "5")
//                .commit();

        /*
         * *压缩 metadata 文件
         *
         * 会生成一组新的
         * v18.metadata.json，
         * snap-3251679826367860830-1-16190a72-c87d-49f1-81b4-7feb0842b285.avro
         * 16190a72-c87d-49f1-81b4-7feb0842b285-m0.avro
         * 16190a72-c87d-49f1-81b4-7feb0842b285-m1.avro
         */
        /*
        table.rewriteManifests()
                .rewriteIf(file -> file.length() < 10 * 1024 * 1024) // 10 MB
                .clusterBy(file -> file.partition().get(0, String.class))
                .commit();
        */


        /*
         * 清理过期的 snapshot, 也会自动清理掉有些没用的 data 文件； data 文件在压缩时并没有删除
         * 多余的 snap-7076638441175417889-1-ea1fddf9-ad4e-4080-90ad-b8454677cf28.avro 文件
         * 在多余 snapshot 文件中关联的 mainlists 文件：ea1fddf9-ad4e-4080-90ad-b8454677cf28-m0.avro
         * mainlists 文件中关联的 data下的 parquet 文件：00000-0-81a5a01c-03bb-434f-ad23-44ca770301ba-00001.parquet

         */
//        ExecutorService executorService = Executors.newSingleThreadExecutor();
//        long current = table.currentSnapshot().timestampMillis();
//        table.expireSnapshots()
//                .expireOlderThan(current)
//                .retainLast(1)
//                .commit();
    }
}
