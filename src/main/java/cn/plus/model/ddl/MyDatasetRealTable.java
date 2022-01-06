package cn.plus.model.ddl;

import java.io.Serializable;

public class MyDatasetRealTable implements Serializable {
    private static final long serialVersionUID = 5902850768322700758L;

    private Long id;
    private String table_name;
    private Long dataset_id;

    public MyDatasetRealTable() {
    }

    public MyDatasetRealTable(final Long id, final String table_name, final Long dataset_id)
    {
        this.id = id;
        this.table_name = table_name;
        this.dataset_id = dataset_id;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public Long getDataset_id() {
        return dataset_id;
    }

    public void setDataset_id(Long dataset_id) {
        this.dataset_id = dataset_id;
    }
}




























































