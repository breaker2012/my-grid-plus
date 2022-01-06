package cn.plus.model;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

/**
 * 记录场景操作的表
 * */
public class MyScenesLog implements Serializable {
    private static final long serialVersionUID = 6344306849899833997L;

    private Long id;
    private byte[] mycacheex;
    private Timestamp create_date;

    public MyScenesLog(final Long id, final byte[] mycacheex)
    {
        this.id = id;
        this.mycacheex = mycacheex;
        this.create_date = new Timestamp((new Date()).getTime());
    }

    public MyScenesLog()
    {
        this.create_date = new Timestamp((new Date()).getTime());
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public byte[] getMycacheex() {
        return mycacheex;
    }

    public void setMycacheex(byte[] mycacheex) {
        this.mycacheex = mycacheex;
    }

    public Timestamp getCreate_date() {
        return create_date;
    }

    public void setCreate_date(Timestamp create_date) {
        this.create_date = create_date;
    }
}



















































