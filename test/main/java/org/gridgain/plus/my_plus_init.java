package org.gridgain.plus;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.gridgain.plus.init.PlusInit;
import org.junit.Test;

public class my_plus_init {

    @Test
    public void show() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);
        PlusInit plusInit = new PlusInit(ignite);
        plusInit.initialization();
        System.out.println("OK");
    }

    @Test
    public void show_1() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyGridGain/my-grid-plus/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);
        String sql = "CREATE TABLE IF NOT EXISTS my_cron (" +
                "                      cron_name VARCHAR(40)," +
                "                      cron VARCHAR," +
                "                      descrip VARCHAR," +
                "                      PRIMARY KEY (cron_name)" +
                "                      ) WITH \"template=MyMeta_template,VALUE_TYPE=cn.plus.model.MyCron,cache_name=my_cron,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";";

        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>("my_meta_table").setSqlSchema("MY_META");
        IgniteCache cache = ignite.getOrCreateCache(cacheCfg);

        cache.query(new SqlFieldsQuery(sql)).getAll();
        System.out.println("OK");
    }

    @Test
    public void show_2() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyGridGain/my-grid-plus/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);
        String sql = "CREATE TABLE IF NOT EXISTS my_func (" +
                "                    method_name VARCHAR(30)," +
                "                    java_method_name VARCHAR(30)," +
                "                    cls_name VARCHAR," +
                "                    return_type VARCHAR(20)," +
                "                    descrip VARCHAR," +
                "                    PRIMARY KEY (method_name)" +
                "                    ) WITH \"template=MyMeta_template,VALUE_TYPE=cn.plus.model.ddl.MyFunc,cache_name=my_func,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";";

        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>("my_meta_table").setSqlSchema("MY_META");
        IgniteCache cache = ignite.getOrCreateCache(cacheCfg);

        cache.query(new SqlFieldsQuery(sql)).getAll();

        sql = "CREATE TABLE IF NOT EXISTS my_func_ps (\n" +
                "                    method_name VARCHAR(30),\n" +
                "                    ps_index INTEGER,\n" +
                "                    ps_type VARCHAR(20),\n" +
                "                    PRIMARY KEY (method_name, ps_index)\n" +
                "                    ) WITH \"template=MyMeta_template,affinityKey=method_name,VALUE_TYPE=cn.plus.model.ddl.MyFuncPs,cache_name=my_func_ps,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";";
        cache.query(new SqlFieldsQuery(sql)).getAll();
        System.out.println("OK");
    }

    @Test
    public void str_format()
    {
        String rs = String.format("(re-find #^(?i)%s\\s+view\\s+ %s)", "吴大富", "美羊羊");
        System.out.println(rs);
    }
}
