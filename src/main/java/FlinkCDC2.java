import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import deserializer.CustomMysqlDeserialization;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.util.Properties;

/**
 * 使用自定义的 反序列化器
 * @author sunshuxian
 * @description
 * @date 2023/5/16
 */
public class FlinkCDC2 {
    public static void main(String[] args) throws Exception {
        // 获取flink的运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        environment.setParallelism(1);


//        // 开启checkpoint
//        // 5 秒
//        environment.enableCheckpointing(5000);
//        // 10秒超时
//        environment.getCheckpointConfig().setCheckpointTimeout(10000);
//        // 精准一次性
//        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        // 最大并发
//        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        // 状态后端地址
////        environment.setStateBackend(new FsStateBackend("hdfs://haddop102:8020/cdc-test/ck"));
//        environment.setStateBackend(new FsStateBackend("file:///cdc-test/ck"));
//


        Properties properties = new Properties();
        properties.put("database.allowPublicKeyRetrieval", "true");
        properties.put("database.useSSL", "true");


        MySqlSource.Builder<String> builder = MySqlSource.builder();
        DebeziumSourceFunction<String> mysqlSourceFunction = builder
                .hostname("localhost")
                .port(3306)
                .username("root")
                //window密码
                .password("ZKlHd9ecTl0Z")
                // mac密码
//                .password("L2Bs9fD#")
                // 监听的库，可以是多个
                .databaseList("test")
                // 监听的表，可以是多个，必须是库名.表名
                .tableList("test.test1")
                // 一定义反序列化器
                .deserializer(new CustomMysqlDeserialization())
                .startupOptions(StartupOptions.initial())
                // 所有链接这个数据库实例的链接的唯一值
                .serverId(1)
                .serverTimeZone("UTC")
                .debeziumProperties(properties)
                .build();
        DataStreamSource<String> stream = environment.addSource(mysqlSourceFunction);
        stream.print();
        environment.execute("FLINK CDC");


    }
}
