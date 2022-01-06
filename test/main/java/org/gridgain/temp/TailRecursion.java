package org.gridgain.temp;

import java.util.stream.Stream;

/**
 * 参考：https://zhuanlan.zhihu.com/p/373949704
 * */
@FunctionalInterface
public interface TailRecursion<T> {

    /**
     * 用于递归栈幀之间的连接，惰性求值
     *
     * @return 返回下一个栈幀
     * */
    TailRecursion<T> apply();

    /**
     * 判断当前递归是否结束
     *
     * @return 默认为 false, 因为正常的递归过程中都还未结束
     * */
    default boolean isFinished()
    {
        return false;
    }

    /**
     * 获得递归结果，只有在递归结束时才能调用，结果默认给出异常，通过工具类的重写来获取值
     * */
    default T getResult()
    {
        throw new Error("递归还没有结束，调用获得结果异常！");
    }

    /**
     * 及早求值，执行一系列的递归，因为栈幀只有一个，所以使用 findFirst 获得最终的栈幀，接着调用 getResult 方法获取最终的值
     * */
    default T invoke()
    {
        return Stream.iterate(this, TailRecursion::apply)
                .filter(TailRecursion::isFinished)
                .findFirst()
                .get()
                .getResult();
    }
}
