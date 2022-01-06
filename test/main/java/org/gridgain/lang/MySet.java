package org.gridgain.lang;

import clojure.lang.PersistentHashSet;
import org.junit.Test;

public class MySet {

    @Test
    public void set_test()
    {
        Object[] objs = new Object[]{"a", "b", "c"};
        PersistentHashSet persistentHashSet = PersistentHashSet.create(objs);

        persistentHashSet.stream().forEach(m -> System.out.println(m));
    }

    @Test
    public void test_1()
    {
        String[] a = new String[2];
        Object[] b = a;
        a[0] = "hi";
        b[1] = Integer.valueOf(42);
        System.out.println(b);
    }
}



























