package org.gridgain.plus;

import org.junit.Test;
import org.locationtech.jts.geom.LineSegment;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * 参考 https://blog.csdn.net/Rolandcoder/article/details/85317054
 * */
public class MyConvert_case {

    @Test
    public void test_1()
    {
        //final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date date = simpleDateFormat.parse("2021-2-01");
            Timestamp timestamp = new Timestamp(date.getTime());
            System.out.println(timestamp);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    private Timestamp getTimestamp(final String t, final List<SimpleDateFormat> lst, final int index)
    {
        try {
            Date date = lst.get(index).parse(t);
            Timestamp timestamp = new Timestamp(date.getTime());
            return timestamp;
        } catch (ParseException e) {
            e.printStackTrace();
            if (index < lst.size()) {
                return getTimestamp(t, lst, index + 1);
            }
            return null;
        }
    }

    public Timestamp ConvertToTimestamp(final String t) throws Exception {
        List<SimpleDateFormat> lst = Arrays.asList(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
                new SimpleDateFormat("yyyy-MM-dd"),
                new SimpleDateFormat("yyyy/MM/dd HH:mm:ss"),
                new SimpleDateFormat("yyyy/MM/dd"),
                new SimpleDateFormat("yyyyMMdd HH:mm:ss"),
                new SimpleDateFormat("yyyyMMddHHmmssSSS"),
                new SimpleDateFormat("yyyyMMddHHmmss"),
                new SimpleDateFormat("yyyyMMdd"));
        Timestamp timestamp = getTimestamp(t, lst, 0);
        if (timestamp != null)
        {
            return timestamp;
        }
        throw new Exception("字符串转换失败，请检查输入字符串！");
    }

    @Test
    public void test_2()
    {
        try {
            Timestamp timestamp = ConvertToTimestamp("20181227182439000");
            System.out.println(timestamp);
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test_3()
    {
        List<Integer> lst = Arrays.asList(1, 2, 3, 4, 5);
        for (int i = 0; i < lst.size(); i++)
        {
            if (i < lst.size())
            {
                System.out.println(lst.get(i));
            }
        }
    }
}
























































