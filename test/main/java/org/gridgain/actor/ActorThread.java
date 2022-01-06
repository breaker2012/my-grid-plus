package org.gridgain.actor;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 核心功能很简单, 就是每个Actor是一个线程处理自身事件队列的循环,
 * Actor中定义的数据和方法只在该线程中访问, Actor实现的接口可公开出来供其它Actor自动异步访问(通过动态代理).
 * */
@SuppressWarnings("MethodMayBeStatic")
public abstract class ActorThread extends Thread {
    private static final class Message {
        final CompletableFuture<Object> future = new CompletableFuture<>();
        final ActorThread threadFrom;
        final Method method;
        final Object[] params;
        Object result;

        Message(Thread threadFrom, Method method, Object[] params) {
            this.threadFrom = threadFrom instanceof ActorThread ? (ActorThread)threadFrom : null;
            this.method = method;
            this.params = params;
        }
    }

    private static final ConcurrentHashMap<Class<?>, Object> actorMap = new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<Message> msgQueue = new LinkedBlockingQueue<>();

    protected ActorThread() {
        final Class<? extends ActorThread> cls = getClass();
        final Class<?>[] intfs = cls.getInterfaces();
        if (intfs.length <= 0)
            throw new IllegalStateException("no actor in " + cls.getName());
        final Object proxy = Proxy.newProxyInstance(cls.getClassLoader(), intfs, (__, method, params) -> {
            final Message msg = new Message(Thread.currentThread(), method, params);
            msgQueue.put(msg);
            //noinspection SuspiciousInvocationHandlerImplementation
            return msg.future;
        });
        for (final Class<?> intf : intfs) {
            final Object oldActor = actorMap.putIfAbsent(intf, proxy);
            if (oldActor != null)
                throw new IllegalStateException("duplicated actor " + intf.getName()
                        + " in " + cls.getName() + " and " + oldActor.getClass().getName());
        }
        start();
    }

    public static void init(Class<? extends ActorThread> actorClass) throws Exception {
        final Constructor<? extends ActorThread> ctor = actorClass.getDeclaredConstructor();
        ctor.setAccessible(true);
        ctor.newInstance();
    }

    public static <I> I getActor(Class<I> interfaceClass) {
        //noinspection unchecked
        return (I)actorMap.get(interfaceClass);
    }

    protected boolean onInterrupted() {
        return true;
    }

    protected void onException(Throwable e) {
        e.printStackTrace();
    }

    @Override
    public void run() {
        for (; ; ) {
            try {
                //noinspection InfiniteLoopStatement
                for (; ; ) {
                    final Message msg = msgQueue.take();
                    Object res = msg.result;
                    if (res == null) {
                        res = msg.method.invoke(this, msg.params);
                        if (res instanceof CompletableFuture)
                            res = ((CompletableFuture<?>)res).getNow(null);
                        if (msg.threadFrom != null) {
                            msg.result = res != null ? res : msgQueue;
                            msg.threadFrom.msgQueue.put(msg);
                        } else
                            msg.future.complete(res);
                    } else
                        msg.future.complete(res != msgQueue ? res : null);
                }
            } catch (InterruptedException e) {
                //noinspection ResultOfMethodCallIgnored
                interrupted(); // clear status
                if (onInterrupted())
                    break;
            } catch (Throwable e) {
                onException(e);
            }
        }
    }
}
