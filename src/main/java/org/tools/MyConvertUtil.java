package org.tools;

import org.gridgain.dml.util.MyCacheExUtil;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class MyConvertUtil {

    public static byte[] ConvertToByte(final Object obj)
    {
        return MyCacheExUtil.objToBytes(obj);
    }

    public static Boolean ConvertToBoolean(Object t)
    {
        if (t != null && (((int)t == 1) || ((String)t).toLowerCase().equals("true")))
            return true;
        return false;
    }

    public static Boolean ConvertToBoolean(Integer t)
    {
        if (t != null && t == 1)
            return true;
        return false;
    }

    public static Boolean ConvertToBoolean(Double t)
    {
        if (t != null && t == 1D)
            return true;
        return false;
    }

    public static Boolean ConvertToBoolean(Long t)
    {
        if (t != null && t == 1L)
            return true;
        return false;
    }

    public static Boolean ConvertToBoolean(Float t)
    {
        if (t != null && t == 1F)
            return true;
        return false;
    }

    public static Boolean ConvertToBoolean(String t)
    {
        if (t != null && (t.equals("1") || t.toLowerCase().equals("true")))
            return true;
        return false;
    }

    public static Integer ConvertToInt(Integer t)
    {
        return t;
    }

    public static Integer ConvertToInt(Double t)
    {
        return t.intValue();
    }

    public static Integer ConvertToInt(Long t)
    {
        return t.intValue();
    }

    public static Integer ConvertToInt(Float t)
    {
        return t.intValue();
    }

    public static Integer ConvertToInt(String t)
    {
        return Integer.parseInt(t.toString());
    }

    public static Integer ConvertToInt(Object t)
    {
        if (t instanceof Double)
            return ConvertToInt((Double)t);
        if (t instanceof Long)
            return ConvertToInt((Long)t);
        if (t instanceof Integer)
            return (Integer)t;
        if (t instanceof Float)
            return ConvertToInt((Float)t);
        if (t instanceof String)
            return ConvertToInt((String)t);
        if (t instanceof BigDecimal)
            return ConvertToInt((BigDecimal)t);
        return null;
    }

    public static Integer ConvertToInt(BigDecimal t)
    {
        return t.intValue();
    }

    public static Long ConvertToLong(Integer t)
    {
        return t.longValue();
    }

    public static Long ConvertToLong(Double t)
    {
        return t.longValue();
    }

    public static Long ConvertToLong(Long t)
    {
        return t;
    }

    public static Long ConvertToLong(BigDecimal t)
    {
        return t.longValue();
    }

    public static Long ConvertToLong(Float t)
    {
        return t.longValue();
    }

    public static Long ConvertToLong(String t)
    {
        return Long.parseLong(t.toString());
    }

    public static Long ConvertToLong(Object t)
    {
        if (t instanceof Double)
            return ConvertToLong((Double)t);
        if (t instanceof Long)
            return (Long)t;
        if (t instanceof Integer)
            return ConvertToLong((Integer)t);
        if (t instanceof Float)
            return ConvertToLong((Float)t);
        if (t instanceof String)
            return ConvertToLong((String)t);
        if (t instanceof BigDecimal)
            return ConvertToLong((BigDecimal)t);
        return null;
    }

    public static Double ConvertToDouble(Integer t)
    {
        return t.doubleValue();
    }

    public static Double ConvertToDouble(Double t)
    {
        return t;
    }

    public static Double ConvertToDouble(Long t)
    {
        return t.doubleValue();
    }

    public static Double ConvertToDouble(Float t)
    {
        return t.doubleValue();
    }

    public static Double ConvertToDouble(String t)
    {
        return Double.parseDouble(t.toString());
    }

    public static Double ConvertToDouble(Object t)
    {
        return Double.parseDouble(t.toString());
    }

    public static Double ConvertToDouble(BigDecimal t)
    {
        return t.doubleValue();
    }

    public static Timestamp ConvertToTimestamp(final Long t) {
        return new Timestamp(t);
    }

    public static Timestamp ConvertToTimestamp(final Object t) {
        return (Timestamp)t;
    }

    public static Timestamp ConvertToTimestamp(final Integer t) {
        return new Timestamp(t.longValue());
    }

    public static Timestamp ConvertToTimestamp(final Double t) {
        return new Timestamp(t.longValue());
    }

    public static Timestamp ConvertToTimestamp(final Float t) {
        return new Timestamp(t.longValue());
    }

    /**
     * String 转 Timestamp
     * */
    public static Timestamp ConvertToTimestamp(final String t, final String format) {
        //final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        try {
            Date date = simpleDateFormat.parse(t);
            return new Timestamp(date.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static Timestamp getTimestamp(final String t, final List<SimpleDateFormat> lst, final int index)
    {
        try {
            Date date = lst.get(index).parse(t);
            Timestamp timestamp = new Timestamp(date.getTime());
            return timestamp;
        } catch (ParseException e) {
            if (index < lst.size()) {
                return getTimestamp(t, lst, index + 1);
            }
            return null;
        }
    }

    public static Timestamp ConvertToTimestamp(final String t) throws Exception {
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

    public static String ConvertToString(final Integer t)
    {
        return t.toString();
    }

    public static String ConvertToString(final Object t)
    {
        return t.toString();
    }

    public static String ConvertToString(final Long t)
    {
        return t.toString();
    }

    public static String ConvertToString(final Float t)
    {
        return t.toString();
    }

    public static String ConvertToString(final Double t)
    {
        return t.toString();
    }

    /**
     * Timestamp 转 String
     * */
    public static String ConvertToString(final Timestamp t, final String format) {
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        return simpleDateFormat.format(new Date(t.getTime()));
    }

    public static <T> T ConvertToType(final String itemType, final String itemName) throws Exception {
        switch (itemType)
        {
            case "String":
                return (T) ConvertToString(itemName);
            case "Integer":
                return (T) ConvertToInt(itemName);
            case "Long":
                return (T) ConvertToLong(itemName);
            case "Double":
                return (T) ConvertToDouble(itemName);
            case "Boolean":
                return (T) ConvertToBoolean(itemName);
            case "Decimal":
                return (T) ConvertToDecimal(itemName);
            case "Timestamp":
                return (T) ConvertToTimestamp(itemName);
        }
        return null;
    }

    public static BigDecimal ConvertToDecimal(final Integer t)
    {
        return new BigDecimal(t.toString());
    }

    public static BigDecimal ConvertToDecimal(final String t)
    {
        return new BigDecimal(t);
    }

    public static BigDecimal ConvertToDecimal(final Double t)
    {
        return BigDecimal.valueOf(t);
    }

    public static BigDecimal ConvertToDecimal(final Long t)
    {
        return BigDecimal.valueOf(t);
    }

    public static BigDecimal ConvertToDecimal(final Object t)
    {
        return new BigDecimal(t.toString());
    }

    /**
     * 判断列表是否为空
     * */
    public static <T> boolean isNotNull(final List<T> lst)
    {
        if (lst != null && lst.size() > 0)
        {
            return true;
        }
        return false;
    }
}
