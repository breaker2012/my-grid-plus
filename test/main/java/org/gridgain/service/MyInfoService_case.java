package org.gridgain.service;

import org.junit.Test;

public class MyInfoService_case {

    @Test
    public void test1()
    {
        String s = MyInfoService.getInstance().getMyInfo().showMsg("吴大富");
        System.out.println(s);
    }
}
