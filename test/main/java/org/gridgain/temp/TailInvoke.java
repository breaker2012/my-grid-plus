package org.gridgain.temp;

public class TailInvoke {

    /**
     * 获取当前递归的下一个递归
     * */
    public static <T> TailRecursion<T> call(final TailRecursion<T> nextFrame)
    {
        return nextFrame;
    }

    /**
     * 结束当前递归，重写对应的默认方法的值，完成状态改为 true,设置最终返回结果
     * */
    public static <T> TailRecursion<T> done(T value)
    {
        return new TailRecursion<T>() {
            @Override
            public TailRecursion<T> apply() {
                throw new Error("递归已经结束，非法调用 apply 方法");
            }

            @Override
            public boolean isFinished()
            {
                return true;
            }

            @Override
            public T getResult()
            {
                return value;
            }
        };
    }
}















































