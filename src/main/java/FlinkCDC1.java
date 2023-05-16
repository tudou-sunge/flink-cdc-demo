import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author sunshuxian
 * @description
 * @date 2023/5/16
 */
public class FlinkCDC1 {
    public static void main(String[] args) {
        // 获取flink的运行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        executionEnvironment.setParallelism(1);

        MySqlSource.Builder<String> builder = MySqlSource.builder();
        builder.hostname("localhost")
                .port(3306)
                .username("root")
                .password("L2Bs9fD#")
                // 监听的库，可以是多个
                .databaseList("test")
                // 监听的表，可以是多个，必须是库名.表名
                .tableList("test.test1")
                // 反序列化器
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                // 所有链接这个数据库的链接的唯一值
                .serverId(1)
                .build();

    }
}
