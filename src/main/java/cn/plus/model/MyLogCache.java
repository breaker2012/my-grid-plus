package cn.plus.model;

import java.io.Serializable;
import java.util.List;

public class MyLogCache implements Serializable {
    private static final long serialVersionUID = -5518188600794814902L;

    /**
     * 表名
     * */
    private String cache_name;
    /**
     * key 如果是单独的主键，则是基础类型，如果是联合主键，则是 List<MyKeyValue> 类型
     * */
    private Object key;
    private List<MyKeyValue> value;
    private SqlType sqlType;

    public MyLogCache(final String cache_name, final Object key, final List<MyKeyValue> value, final SqlType sqlType)
    {
        this.cache_name = cache_name;
        this.key = key;
        this.value = value;
        this.sqlType = sqlType;
    }

    public MyLogCache()
    {}

    public String getCache_name() {
        return cache_name;
    }

    public void setCache_name(String cache_name) {
        this.cache_name = cache_name;
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public List<MyKeyValue> getValue() {
        return value;
    }

    public void setValue(List<MyKeyValue> value) {
        this.value = value;
    }

    public SqlType getSqlType() {
        return sqlType;
    }

    public void setSqlType(SqlType sqlType) {
        this.sqlType = sqlType;
    }
}
