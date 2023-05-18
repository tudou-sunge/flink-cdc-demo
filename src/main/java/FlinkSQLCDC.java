import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
public class FlinkSQLCDC {
    public static void main(String[] args) throws Exception {
        // 获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE user_info ( \n" +
                "id BIGINT primary key, \n" +
                "user_name STRING,\n" +
                "age INT\n" +
                ") WITH (\n" +
                "'connector' = 'mysql-cdc',\n"+
                // "initial", "earliest-offset", "latest-offset", "specific-offset" and "timestamp".
                "'scan.startup.mode' = 'initial',\n" +
                "'hostname' = 'localhost',\n" +
                "'port' = '3306',\n" +
                "'username' = 'root',\n" +
                "'password' = 'ZKlHd9ecTl0Z',\n" +
                "'database-name' = 'test',\n" +
                "'table-name' = 'test1'\n" +
                ")");
        Table table = tableEnv.sqlQuery("select * from user_info");
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();

        env.execute("FLINK CDC SQL");

    }
}
