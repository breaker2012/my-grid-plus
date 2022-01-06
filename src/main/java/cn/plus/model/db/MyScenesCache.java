package cn.plus.model.db;

import clojure.lang.PersistentArrayMap;
import cn.plus.model.tmp.MScenesCache;

import java.io.Serializable;
import java.util.List;

/**
 * 对应于 my_scenes 表
 * */
public class MyScenesCache implements Serializable {
    private static final long serialVersionUID = -7489535188547987509L;

    // 用户组名称
    private Long group_id;
    // 场景名字 （主键id）
    private String scenes_name;
    // 场景 code
    private String sql_code;
    // 场景描述
    private String descrip;
    // 是否是处理
    private Boolean is_batch;
    // 参数
    private List<MyScenesParams> params;
    // ast 语法树
    private PersistentArrayMap ast;

    public MyScenesCache(final Long group_id, final String scenes_name, final String sql_code, final String descrip, final Boolean is_batch, final List<MyScenesParams> params, final PersistentArrayMap ast)
    {
        this.group_id = group_id;
        this.scenes_name = scenes_name;
        this.sql_code = sql_code;
        this.descrip = descrip;
        this.is_batch = is_batch;
        this.params = params;
        this.ast = ast;
    }

    public MyScenesCache(final MScenesCache m, final PersistentArrayMap ast)
    {
        this.group_id = m.getGroup_id();
        this.scenes_name = m.getScenes_name();
        this.sql_code = m.getSql_code();
        this.descrip = m.getDescrip();
        this.is_batch = m.getIs_batch();
        this.params = m.getParams();
        this.ast = ast;
    }

    public MyScenesCache()
    {}

    public Long getGroup_id() {
        return group_id;
    }

    public void setGroup_id(Long group_id) {
        this.group_id = group_id;
    }

    public String getScenes_name() {
        return scenes_name;
    }

    public void setScenes_name(String scenes_name) {
        this.scenes_name = scenes_name;
    }

    public String getSql_code() {
        return sql_code;
    }

    public void setSql_code(String sql_code) {
        this.sql_code = sql_code;
    }

    public String getDescrip() {
        return descrip;
    }

    public void setDescrip(String descrip) {
        this.descrip = descrip;
    }

    public Boolean getIs_batch() {
        return is_batch;
    }

    public void setIs_batch(Boolean is_batch) {
        this.is_batch = is_batch;
    }

    public List<MyScenesParams> getParams() {
        return params;
    }

    public void setParams(List<MyScenesParams> params) {
        this.params = params;
    }

    public PersistentArrayMap getAst() {
        return ast;
    }

    public void setAst(PersistentArrayMap ast) {
        this.ast = ast;
    }
}







































