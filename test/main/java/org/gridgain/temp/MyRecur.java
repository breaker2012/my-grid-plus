package org.gridgain.temp;

import org.junit.Test;

public class MyRecur {

    /**
     * 测试递归累加
     * */
    @Test
    public void recur_test()
    {
        MyAdd myAdd = new MyAdd(100L);
        System.out.println(myAdd.recur_tail_add());
        //System.out.println(myAdd.recur_add(10000L));
        Long result = myAdd.recur(100000L, 0L).invoke();
        System.out.println(result);
    }

}
