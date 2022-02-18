package org.gridgain.dml.util;

import cn.plus.model.MyCacheEx;
import cn.plus.model.MyKeyValue;
import cn.plus.model.MyLogCache;
import org.tools.KvSql;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.tools.MyLineToBinary;

import java.io.*;
import java.util.List;

public class MyCacheExUtil implements Serializable {
    private static final long serialVersionUID = 7714300623488330841L;

    /**
     * MyLogCache 转换到 MyCacheEx
     * */
    public static MyCacheEx convertToCacheEx(final Ignite ignite, final MyLogCache logCache)
    {
        switch (logCache.getSqlType())
        {
            case INSERT:
                return new MyCacheEx(ignite.cache(logCache.getCache_name()), convertToKey(ignite, logCache), convertToValue(ignite, logCache), logCache.getSqlType());
            case DELETE:
                return new MyCacheEx(ignite.cache(logCache.getCache_name()), convertToKey(ignite, logCache), null, logCache.getSqlType());
            case UPDATE:
                Object key = convertToKey(ignite, logCache);
                IgniteCache igniteCache = ignite.cache(logCache.getCache_name()).withKeepBinary();
                BinaryObject binaryObject = (BinaryObject) igniteCache.get(key);
                BinaryObjectBuilder binaryObjectBuilder = binaryObject.toBuilder();
                for (MyKeyValue m : logCache.getValue())
                {
                    binaryObjectBuilder.setField(m.getName(), m.getValue());
                }
                return new MyCacheEx(igniteCache, key, binaryObjectBuilder.build(), logCache.getSqlType());
        }
        return null;
    }

    public static MyCacheEx convertToCacheEx(final Ignite ignite, final byte[] bytes)
    {
        return convertToCacheEx(ignite, (MyLogCache)MyCacheExUtil.restore(bytes));
    }

    public static byte[] mycacheToBytes(final MyLogCache myLogCache)
    {
        return objToBytes(myLogCache);
    }

    public static MyLogCache restoreMyCache(final byte[] bytes)
    {
        return (MyLogCache)restore(bytes);
    }

    public static BinaryObject getValues(final Ignite ignite, final String table_name, final List<MyKeyValue> lst)
    {
        BinaryObjectBuilder valueBuilder = ignite.binary().builder(KvSql.getValueType(ignite, table_name));
        for (MyKeyValue m : lst)
        {
            valueBuilder.setField(m.getName(), m.getValue());
        }
        return valueBuilder.build();
    }

    public static BinaryObject getKeys(final Ignite ignite, final String table_name, final List<MyKeyValue> lst)
    {
        BinaryObjectBuilder keyBuilder = ignite.binary().builder(KvSql.getKeyType(ignite, table_name));
        for (MyKeyValue m : lst)
        {
            keyBuilder.setField(m.getName(), m.getValue());
        }
        return keyBuilder.build();
    }

    public static Object convertToKey(final Ignite ignite, final MyLogCache myLogCache)
    {
        Object key = null;
        if (myLogCache.getKey() instanceof List) {
            key = MyCacheExUtil.getKeys(ignite, myLogCache.getCache_name(), (List<MyKeyValue>)myLogCache.getKey());
        } else {
            key = myLogCache.getKey();
        }
        return key;
    }

    public static Object convertToValue(final Ignite ignite, final MyLogCache myLogCache)
    {
        Object value = null;
        if (myLogCache.getValue() instanceof List) {
            value = MyCacheExUtil.getValues(ignite, myLogCache.getCache_name(), myLogCache.getValue());
        } else {
            value = myLogCache.getValue();
        }
        return value;
    }

    public static byte[] objToBytes(final Object obj) {
        return MyLineToBinary.objToBytes(obj);
    }

    /**
     * 二进制数组转回对象
     * */
    public static Object restore(final byte[] bytes)
    {
        return MyLineToBinary.restore(bytes);
    }

    public static String restoreToLine(final byte[] bytes)
    {
        return (String)restore(bytes);
    }

//    /**
//     * 对象转变成二进制
//     * */
//    public static byte[] objToBytes(final Object obj) {
//        ByteArrayOutputStream byteArrayOutputStream = null;
//        ObjectOutputStream objectOutputStream = null;
//
//        try {
//            byteArrayOutputStream = new ByteArrayOutputStream();
//            objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
//            objectOutputStream.writeObject(obj);
//            return byteArrayOutputStream.toByteArray();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            if (objectOutputStream != null) {
//                try {
//                    objectOutputStream.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//
//            if (byteArrayOutputStream != null) {
//                try {
//                    byteArrayOutputStream.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        return null;
//    }

//    /**
//     * 二进制数组转回对象
//     * */
//    public static Object restore(final byte[] bytes)
//    {
//        ByteArrayInputStream byteArrayInputStream = null;
//        ObjectInputStream objectInputStream = null;
//
//        try {
//            byteArrayInputStream = new ByteArrayInputStream(bytes);
//            objectInputStream = new ObjectInputStream(byteArrayInputStream);
//            return objectInputStream.readObject();
//        } catch (IOException | ClassNotFoundException e) {
//            e.printStackTrace();
//        } finally {
//            if (objectInputStream != null) {
//                try {
//                    objectInputStream.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//
//            if (byteArrayInputStream != null) {
//                try {
//                    byteArrayInputStream.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        return null;
//    }
}











































