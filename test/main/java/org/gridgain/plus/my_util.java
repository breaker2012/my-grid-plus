package org.gridgain.plus;

import org.gridgain.plus.tools.MyUtil;
import org.junit.Test;

import java.util.*;

public class my_util {
    MyUtil util;

    @Test
    public void test_1()
    {
        List<String> lst = new ArrayList<>();
        lst.add("a");
        //String rs = MyUtil.gson().toJson(lst);
        //System.out.println(rs);
    }

    @Test
    public void test_2()
    {
//        ArrayList<MyScenesPs> lst = new ArrayList<MyScenesPs>();
//        lst.add(new MyScenesPs("S".getClass(), 0L));
//        lst.add(new MyScenesPs("S".getClass(), 1L));

        //String rs = MyUtil.gson().toJson(lst);
        //System.out.println(rs);
    }

    public static Integer getUUIDInOrderId(){
        Integer orderId = UUID.randomUUID().toString().hashCode();
        orderId = orderId < 0 ? -orderId : orderId; //String.hashCode() 值会为空
        return orderId;
    }

    @Test
    public void test_4()
    {
        String name = "root";
        String password = "吴大贵";
        System.out.println(name.hashCode());
        System.out.println(password.hashCode());

        System.out.println(UUID.randomUUID().toString().replace("-", ""));
        System.out.println(UUID.randomUUID().toString().hashCode());

        System.out.println(UUID.randomUUID().getLeastSignificantBits());
        System.out.println(UUID.randomUUID().getMostSignificantBits());
        System.out.println(UUID.fromString("吴大富-wudagui").getLeastSignificantBits());
    }

    public void test_3()
    {
        Map<String, String> map = new HashMap<>();
        map.put("1", "A");
        map.put("2", "B");
        map.put("3", "C");

        //map.keySet()
        //map.containsKey()
    }
}
