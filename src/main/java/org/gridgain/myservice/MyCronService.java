package org.gridgain.myservice;

import org.gridgain.superservice.IMyCron;

public class MyCronService {
    private IMyCron myCron;

    public IMyCron getMyCron() {
        return myCron;
    }

    private static class InstanceHolder {

        public static MyCronService instance;

        static {
            try {
                instance = new MyCronService();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 获取单例模式
     * */
    public static MyCronService getInstance() {
        return MyCronService.InstanceHolder.instance;
    }

    /**
     * 构造函数设置为私有，只能通过 getInstance() 方法获取
     * */
    private MyCronService() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class<?> cls = Class.forName("org.gridgain.plus.dml.MyCron");
        myCron = (IMyCron) cls.newInstance();
    }
}
