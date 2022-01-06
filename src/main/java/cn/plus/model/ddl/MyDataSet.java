package cn.plus.model.ddl;

import java.io.Serializable;

public class MyDataSet implements Serializable {
    private static final long serialVersionUID = 7116154131226236070L;

    private Long id;
    private String dataset_name;
    private Boolean is_real;

    public MyDataSet()
    {}

    public MyDataSet(final Long id, final String dataset_name, final Boolean is_real)
    {
        this.id = id;
        this.dataset_name = dataset_name;
        this.is_real = is_real;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getDataset_name() {
        return dataset_name;
    }

    public void setDataset_name(String dataset_name) {
        this.dataset_name = dataset_name;
    }

    public Boolean getIs_real() {
        return is_real;
    }

    public void setIs_real(Boolean is_real) {
        this.is_real = is_real;
    }
}
