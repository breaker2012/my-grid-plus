package cn.plus.model;

import clojure.lang.PersistentArrayMap;

import java.io.Serializable;

/**
 * 定时任务
 * */
public class MCron implements Serializable {

    private static final long serialVersionUID = 5363891392457651930L;
    private String cron_name;
    private String cron;
    private String descrip;
    private clojure.lang.PersistentArrayMap ast;

    public MCron(final String cron_name, final String cron, final String descrip, final clojure.lang.PersistentArrayMap ast)
    {
        this.cron_name = cron_name;
        this.cron = cron;
        this.descrip = descrip;
        this.ast = ast;
    }

    public MCron()
    {}

    public PersistentArrayMap getAst() {
        return ast;
    }

    public void setAst(PersistentArrayMap ast) {
        this.ast = ast;
    }

    public String getCron_name() {
        return cron_name;
    }

    public void setCron_name(String cron_name) {
        this.cron_name = cron_name;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public String getDescrip() {
        return descrip;
    }

    public void setDescrip(String descrip) {
        this.descrip = descrip;
    }
}
