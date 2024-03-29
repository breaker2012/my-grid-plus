package cn.plus.model.ddl;

import java.io.Serializable;

/**
 * 对应表 my_delete_views
 * */
public class MyDeleteViews implements Serializable {

    private static final long serialVersionUID = -3815963550401273521L;

    private Long id;
    private String view_name;
    private String table_name;
    private Long data_set_id;
    private String code;

    public MyDeleteViews(final Long id, final String view_name, final String table_name, final Long data_set_id, final String code)
    {
        this.id = id;
        this.view_name = view_name;
        this.table_name = table_name;
        this.data_set_id = data_set_id;
        this.code = code;
    }

    public MyDeleteViews()
    {}

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getView_name() {
        return view_name;
    }

    public void setView_name(String view_name) {
        this.view_name = view_name;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public Long getData_set_id() {
        return data_set_id;
    }

    public void setData_set_id(Long data_set_id) {
        this.data_set_id = data_set_id;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }
}

















































