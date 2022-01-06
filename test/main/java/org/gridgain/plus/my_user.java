package org.gridgain.plus;

import clojure.lang.Obj;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgnitionEx;
import org.gridgain.plus.user.MyUser;
import org.junit.Test;

import java.util.List;

public class my_user {

    @Test
    public void test_1() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);
        MyUser myUser = new MyUser(ignite);
//        List<?> lst = myUser.login("吴大富", "abc", "file:///Users/chenfei/temp/grid");
//        if (lst != null)
//        {
//            for (Object row : lst)
//            {
//                System.out.println(row.toString());
//            }
//        }
    }

    @Test
    public void test_2() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);
        MyUser myUser = new MyUser(ignite);
//        List<?> lst = myUser.login("admin", "admin", "file:///Users/chenfei/temp/grid");
//        if (lst != null)
//        {
//            for (Object row : lst)
//            {
//                System.out.println(row.toString());
//            }
//        }
    }

}



































