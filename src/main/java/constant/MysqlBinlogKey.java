package constant;


public class MysqlBinlogKey {

    /**
     * 库名
     */
    public static final String DB_NAME = "dbName";
    /**
     * 表名
     */

    public static final String TABLE_NAME = "tableName";
    /**
     * 主键
     */
    public static final String ID = "id";

    /**
     * 事件类型
     */
    public static final String EVENT_TYPE = "event_type";

    /**
     * 事件时间
     */
    public static final String EVENT_TIME = "eventTime";

    /**
     * BEFORE
     */
    public static final String BEFORE = "before";

    /**
     * AFTER
     */
    public static final String AFTER = "after";


    /**
     * 来源
     */
    public static final String SOURCE = "source";

    /**
     * 来源 库名
     */
    public static final String DB = "db";

    /**
     * 来源  表名
     */
    public static final String TABLE = "table";
    /**
     * 来源  时间戳
     */
    public static final String TS_MS = "ts_ms";
    /**
     * 来源  事件类型
     */
    public static final String OP = "op";

}
