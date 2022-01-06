package org.gridgain.lang;

import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentHashMap;
import com.google.common.base.Strings;
import com.sun.codemodel.internal.JForEach;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MyMap {

    @Test
    public void test_map()
    {
        Map m = new HashMap();
        m.put("userGroupId", 1);
        PersistentHashMap persistentArrayMap = PersistentHashMap.createWithCheck(new Object[] {"userGroupId", "吴大富", 1, "吴大贵"});
//        PersistentHashMap map = PersistentHashMap.create(m);
        //persistentArrayMap.assoc("userGroupId", "吴大富");

        for (Object key: persistentArrayMap.keySet())
        {
            System.out.println(key);
            System.out.println(persistentArrayMap.get(key));
        }
    }

    @Test
    public void my_test_str()
    {
        if (Strings.isNullOrEmpty(""))
        {
            System.out.println("为空");
        }
    }
}













































