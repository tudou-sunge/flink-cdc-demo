package constant;


import org.apache.commons.lang3.StringUtils;

public enum Operate {

    CREATE("c"),
    UPDATE("u"),
    DELETE("d");
    private final String operate;

    Operate(String operate) {
        this.operate = operate;
    }

    public String getOperate() {
        return this.operate;
    }

    public static String getEventType(String value) {
        if (StringUtils.equals(value, CREATE.operate)) {
            return CREATE.name();
        }
        if (StringUtils.equals(value, UPDATE.operate)) {
            return UPDATE.name();
        }
        if (StringUtils.equals(value, DELETE.operate)) {
            return DELETE.name();
        }
        return null;
    }

}
