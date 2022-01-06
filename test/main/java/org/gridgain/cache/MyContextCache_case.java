package org.gridgain.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgnitionEx;
import org.junit.Test;

public class MyContextCache_case {

    @Test
    public void Save() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

//        MyContextCache myContextCache = new MyContextCache(ignite);
//        myContextCache.saveAll();
//
//        myContextCache.saveAllFunc();
//        myContextCache.saveAllScenes();
//        myContextCache.saveAllBuiltin();
    }

    @Test
    public void Read() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

//        MContextCache mContextCache = MyContextCacheUtil.getContextCache(ignite, 1L);
//        System.out.println(mContextCache.getData_set());
//
//        IgniteCache<String, Object> funcCache = ignite.cache("my_meta_cache_all_func");
//        if (funcCache.containsKey("substr"))
//        {
//            System.out.println("存在函数 substr");
//        }
    }
}
