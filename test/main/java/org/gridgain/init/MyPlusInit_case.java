package org.gridgain.init;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.gridgain.plus.init.PlusInit;
import org.junit.Test;

import java.util.List;

public class MyPlusInit_case {

    @Test
    public void init_case() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyGridGain/my-grid-plus/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        PlusInit plusInit = new PlusInit(ignite);
        plusInit.initialization();

        List<List<?>> lst = ignite.cache("my_users_group").query(new SqlFieldsQuery("select auto_id('my_users_group')")).getAll();
        for (List<?> row : lst)
        {
            System.out.println(row.get(0));
        }
    }

    @Test
    public void cache_case() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyGridGain/my-grid-plus/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>("public_meta").setSqlSchema("PUBLIC");
        Class[] cls = cacheCfg.getSqlFunctionClasses();
        System.out.println(cls);
    }
}
